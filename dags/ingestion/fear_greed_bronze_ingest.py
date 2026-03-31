# ---------------------------------------------------------------------------
# WHAT CHANGED (vs fear_greed_pipeline.py):
#   1. Converted from classic DAG + PythonOperator to TaskFlow @dag/@task.
#   2. Moved helpers to utils.api_clients.fear_greed and
#      utils.databricks.connection.
#   3. dag_id renamed from ingest_bronze_fear_greed_index →
#      fear_greed_bronze_ingest.
#   4. default_args hardened: retries=3, sla, email_on_failure.
#   5. Added tags, doc_md.
# ---------------------------------------------------------------------------
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from databricks import sql

from callbacks import task_failure_alert

logger = logging.getLogger(__name__)

DOC_MD = """
### Fear & Greed Bronze Ingestion

Fetches the latest Fear & Greed Index value from
[Alternative.me](https://alternative.me/crypto/fear-and-greed-index/)
and upserts it into the Databricks bronze layer (`fear_greed_raw` Delta table).

**Idempotency:** Uses MERGE on (metric_date, source) — safe to rerun.

**Downstream:** Triggers `data_quality` for bronze/fear_greed checks.
"""

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "email_on_failure": True,
    "on_failure_callback": task_failure_alert,
}


@dag(
    dag_id="fear_greed_bronze_ingest",
    default_args=default_args,
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["bronze", "crypto", "ingestion", "fear_greed"],
    doc_md=DOC_MD,
)
def fear_greed_bronze_ingest():

    @task(execution_timeout=timedelta(minutes=15))
    def ingest_fear_greed_to_bronze(**context) -> str:
        from utils.api_clients.fear_greed import fetch_fear_greed_index
        from utils.databricks.connection import (
            ensure_columns_exist,
            get_databricks_connection_params,
            get_stable_run_key,
            qualified_schema_name,
            qualified_table_name,
        )

        total_start = time.perf_counter()

        api_start = time.perf_counter()
        fg = fetch_fear_greed_index()
        logger.info("Fear & Greed API call completed in %.2fs", time.perf_counter() - api_start)

        ingestion_ts = datetime.now(timezone.utc)
        run_key = get_stable_run_key(default_prefix="fear_greed_bronze_ingest")
        source = "alternative_me_api"

        connection_params = get_databricks_connection_params()
        catalog = os.getenv("DATABRICKS_CATALOG", "main")
        schema = os.getenv("DATABRICKS_SCHEMA", "bronze")
        table = os.getenv("DATABRICKS_FEAR_GREED_TABLE", "fear_greed_raw")
        schema_name = qualified_schema_name(catalog, schema)
        table_name = qualified_table_name(catalog, schema, table)

        try:
            db_start = time.perf_counter()
            with sql.connect(
                server_hostname=connection_params["host"],
                http_path=connection_params["http_path"],
                access_token=connection_params["token"],
            ) as connection:
                logger.info("Databricks connection in %.2fs", time.perf_counter() - db_start)
                with connection.cursor() as cursor:
                    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                    cursor.execute(f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            value INT, value_classification STRING,
                            metric_date DATE, time_until_update INT,
                            raw_json STRING, ingestion_ts TIMESTAMP,
                            source STRING, run_key STRING
                        ) USING DELTA
                    """)
                    ensure_columns_exist(cursor, table_name, [
                        ("value", "INT"), ("value_classification", "STRING"),
                        ("metric_date", "DATE"), ("time_until_update", "INT"),
                        ("raw_json", "STRING"), ("ingestion_ts", "TIMESTAMP"),
                        ("source", "STRING"), ("run_key", "STRING"),
                    ])

                    cursor.execute(f"""
                        MERGE INTO {table_name} AS target
                        USING (
                            SELECT ? AS value, ? AS value_classification,
                                   ? AS metric_date, ? AS time_until_update,
                                   ? AS raw_json, ? AS ingestion_ts,
                                   ? AS source, ? AS run_key
                        ) AS source
                        ON target.metric_date = source.metric_date
                           AND target.source = source.source
                        WHEN MATCHED AND source.ingestion_ts >= target.ingestion_ts THEN
                          UPDATE SET
                            target.value = source.value,
                            target.value_classification = source.value_classification,
                            target.time_until_update = source.time_until_update,
                            target.raw_json = source.raw_json,
                            target.ingestion_ts = source.ingestion_ts,
                            target.source = source.source,
                            target.run_key = source.run_key
                        WHEN NOT MATCHED THEN INSERT (
                            value, value_classification, metric_date,
                            time_until_update, raw_json, ingestion_ts,
                            source, run_key
                        ) VALUES (
                            source.value, source.value_classification,
                            source.metric_date, source.time_until_update,
                            source.raw_json, source.ingestion_ts,
                            source.source, source.run_key
                        )
                    """, [
                        fg["value"], fg["value_classification"],
                        fg["metric_date"], fg["time_until_update"],
                        fg["raw_json"], ingestion_ts, source, run_key,
                    ])
        except Exception as exc:
            if "403" in str(exc).lower() or "forbidden" in str(exc).lower():
                raise RuntimeError(
                    "Databricks 403 Forbidden. Verify host, token, and http_path."
                ) from exc
            raise

        logger.info(
            "Pipeline complete in %.2fs — value=%s, classification=%s (run_key=%s)",
            time.perf_counter() - total_start, fg["value"], fg["value_classification"], run_key,
        )
        return run_key

    run_key = ingest_fear_greed_to_bronze()

    trigger_quality = TriggerDagRunOperator(
        task_id="trigger_quality_after_ingestion",
        trigger_dag_id="data_quality",
        wait_for_completion=False,
        conf={
            "layer": "bronze",
            "bronze_source": "fear_greed",
            "run_key": "{{ ti.xcom_pull(task_ids='ingest_fear_greed_to_bronze') }}",
            "fail_on_non_critical": False,
        },
    )

    run_key >> trigger_quality


fear_greed_bronze_ingest()
