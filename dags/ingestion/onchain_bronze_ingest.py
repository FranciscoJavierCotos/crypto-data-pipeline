# ---------------------------------------------------------------------------
# WHAT CHANGED (vs blockchain_onchain_pipeline.py):
#   1. Converted from classic DAG + PythonOperator to TaskFlow @dag/@task.
#   2. Moved helpers to utils.api_clients.blockchain and
#      utils.databricks.connection.
#   3. dag_id renamed from ingest_bronze_blockchain_onchain_metrics →
#      onchain_bronze_ingest.
#   4. default_args hardened: retries=3, sla, email_on_failure.
#   5. Added tags, doc_md.
# ---------------------------------------------------------------------------
from __future__ import annotations

import json
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
### On-chain Bronze Ingestion

Fetches Bitcoin on-chain metrics (transaction count, unique addresses,
hash rate, mempool size) from
[Blockchain.com Charts API](https://www.blockchain.com/charts) and loads
them into the Databricks bronze layer (`onchain_macro_raw` Delta table).

**Idempotency:** DELETE by (source, metric_date range) + INSERT.

**Downstream:** Triggers `data_quality` for bronze/onchain checks.
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
    dag_id="onchain_bronze_ingest",
    default_args=default_args,
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["bronze", "crypto", "ingestion", "onchain"],
    doc_md=DOC_MD,
)
def onchain_bronze_ingest():

    @task(execution_timeout=timedelta(minutes=20))
    def ingest_onchain_to_bronze(**context) -> str:
        from utils.api_clients.blockchain import fetch_onchain_metrics
        from utils.databricks.connection import (
            ensure_columns_exist,
            get_databricks_connection_params,
            get_stable_run_key,
            insert_records_in_batches,
            qualified_schema_name,
            qualified_table_name,
        )
        from utils.validators.payload import safe_int

        total_start = time.perf_counter()
        timespan_days = safe_int(os.getenv("BLOCKCHAIN_CHART_TIMESPAN_DAYS")) or 120
        source = "blockchain_info_charts_api"
        ingestion_ts = datetime.now(timezone.utc)
        run_key = get_stable_run_key(default_prefix="onchain_bronze_ingest")

        api_start = time.perf_counter()
        combined_by_date, payload_cache = fetch_onchain_metrics(timespan_days)
        logger.info(
            "On-chain API calls completed in %.2fs (%d dates)",
            time.perf_counter() - api_start, len(combined_by_date),
        )

        column_names = [
            "metric_date", "btc_tx_count", "btc_unique_addresses",
            "btc_hash_rate", "btc_mempool_size", "raw_json",
            "ingestion_ts", "source", "run_key",
        ]

        records = []
        for metric_date in sorted(combined_by_date):
            row = combined_by_date[metric_date]
            records.append((
                metric_date,
                row.get("btc_tx_count"),
                row.get("btc_unique_addresses"),
                row.get("btc_hash_rate"),
                row.get("btc_mempool_size"),
                json.dumps({"metric_date": str(metric_date), "values": row}),
                ingestion_ts, source, run_key,
            ))

        connection_params = get_databricks_connection_params()
        catalog = os.getenv("DATABRICKS_CATALOG", "main")
        schema = os.getenv("DATABRICKS_SCHEMA", "bronze")
        table = os.getenv("DATABRICKS_ONCHAIN_TABLE", "onchain_macro_raw")
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
                            metric_date DATE, btc_tx_count DOUBLE,
                            btc_unique_addresses DOUBLE, btc_hash_rate DOUBLE,
                            btc_mempool_size DOUBLE, raw_json STRING,
                            ingestion_ts TIMESTAMP, source STRING,
                            run_key STRING
                        ) USING DELTA
                    """)
                    ensure_columns_exist(cursor, table_name, [
                        ("metric_date", "DATE"), ("btc_tx_count", "DOUBLE"),
                        ("btc_unique_addresses", "DOUBLE"),
                        ("btc_hash_rate", "DOUBLE"),
                        ("btc_mempool_size", "DOUBLE"),
                        ("raw_json", "STRING"),
                        ("ingestion_ts", "TIMESTAMP"),
                        ("source", "STRING"), ("run_key", "STRING"),
                    ])

                    metric_dates = sorted({r[0] for r in records})
                    placeholders = ", ".join(["?"] * len(metric_dates))
                    cursor.execute(
                        f"DELETE FROM {table_name} WHERE source = ? AND metric_date IN ({placeholders})",
                        [source, *metric_dates],
                    )
                    insert_records_in_batches(cursor, table_name, column_names, records, batch_size=200)

        except Exception as exc:
            if "403" in str(exc).lower() or "forbidden" in str(exc).lower():
                raise RuntimeError(
                    "Databricks 403 Forbidden. Verify host, token, and http_path."
                ) from exc
            raise

        logger.info(
            "Pipeline complete in %.2fs — %d on-chain rows (run_key=%s)",
            time.perf_counter() - total_start, len(records), run_key,
        )
        return run_key

    run_key = ingest_onchain_to_bronze()

    trigger_quality = TriggerDagRunOperator(
        task_id="trigger_quality_after_ingestion",
        trigger_dag_id="data_quality",
        wait_for_completion=False,
        conf={
            "layer": "bronze",
            "bronze_source": "onchain",
            "run_key": "{{ ti.xcom_pull(task_ids='ingest_onchain_to_bronze') }}",
            "fail_on_non_critical": False,
        },
    )

    run_key >> trigger_quality


onchain_bronze_ingest()
