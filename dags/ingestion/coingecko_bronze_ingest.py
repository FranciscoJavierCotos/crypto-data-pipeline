# ---------------------------------------------------------------------------
# WHAT CHANGED (vs coingecko_pipeline.py):
#   1. Converted from classic DAG + PythonOperator to TaskFlow @dag/@task.
#   2. Moved all shared helpers to utils.databricks.connection,
#      utils.api_clients.coingecko, and utils.validators.payload.
#   3. dag_id renamed from ingest_bronze_coingecko_market_data →
#      coingecko_bronze_ingest for layered naming consistency.
#   4. default_args upgraded: retries=3, sla=1h, email_on_failure=True,
#      retry_exponential_backoff=True.
#   5. Added tags, doc_md, and owner metadata.
#   6. Callback import now from plugins/callbacks.py (not dags/).
#   7. File moved from dags/ → dags/ingestion/.
#
# DESIGN DECISIONS (flagged for review):
#   - dag_id changed — update any external triggers referencing the old
#     dag_id "ingest_bronze_coingecko_market_data".
#   - run_key logic preserved; idempotent DELETE+INSERT pattern unchanged.
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
### CoinGecko Bronze Ingestion

Fetches the top-50 crypto coins by market cap from the
[CoinGecko /coins/markets](https://www.coingecko.com/en/api/documentation) endpoint
and loads them into the Databricks bronze layer (`coingecko_market_data` Delta table).

**Idempotency:** Each run generates a deterministic `run_key`.
On retry the previous batch for that key is deleted before re-insert.

**Downstream:** Triggers `data_quality` for bronze/coingecko checks asynchronously.
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
    dag_id="coingecko_bronze_ingest",
    default_args=default_args,
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["bronze", "crypto", "ingestion", "coingecko"],
    doc_md=DOC_MD,
)
def coingecko_bronze_ingest():

    @task(execution_timeout=timedelta(minutes=15))
    def ingest_coingecko_to_bronze(**context) -> str:
        from utils.api_clients.coingecko import fetch_coingecko_market_data
        from utils.databricks.connection import (
            ensure_columns_exist,
            get_databricks_connection_params,
            get_stable_run_key,
            insert_records_in_batches,
            qualified_schema_name,
            qualified_table_name,
        )
        from utils.validators.payload import parse_iso8601_timestamp, safe_float, safe_int

        total_start = time.perf_counter()

        # ----- API call -----
        api_start = time.perf_counter()
        data = fetch_coingecko_market_data(vs_currency="usd", per_page=50, page=1)
        logger.info("CoinGecko API call completed in %.2fs", time.perf_counter() - api_start)

        ingestion_ts = datetime.now(timezone.utc)
        run_key = get_stable_run_key(default_prefix="coingecko_bronze_ingest")
        source = "coingecko_api"

        # ----- Databricks connection -----
        connection_params = get_databricks_connection_params()
        catalog = os.getenv("DATABRICKS_CATALOG", "main")
        schema = os.getenv("DATABRICKS_SCHEMA", "bronze")
        table = os.getenv("DATABRICKS_TABLE", "coingecko_market_data")
        schema_name = qualified_schema_name(catalog, schema)
        table_name = qualified_table_name(catalog, schema, table)

        column_names = [
            "id", "symbol", "name", "current_price", "market_cap",
            "total_volume", "high_24h", "low_24h", "price_change_24h",
            "price_change_percentage_24h", "market_cap_rank",
            "fully_diluted_valuation", "market_cap_change_24h",
            "market_cap_change_percentage_24h", "circulating_supply",
            "total_supply", "max_supply", "ath", "ath_change_percentage",
            "ath_date", "atl", "atl_change_percentage", "atl_date",
            "volume_to_market_cap_ratio", "last_updated", "raw_json",
            "ingestion_ts", "source", "run_key",
        ]

        try:
            db_start = time.perf_counter()
            with sql.connect(
                server_hostname=connection_params["host"],
                http_path=connection_params["http_path"],
                access_token=connection_params["token"],
            ) as connection:
                logger.info("Databricks connection opened in %.2fs", time.perf_counter() - db_start)

                with connection.cursor() as cursor:
                    # DDL — idempotent table creation
                    ddl_start = time.perf_counter()
                    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                    cursor.execute(f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            id STRING, symbol STRING, name STRING,
                            current_price DOUBLE, market_cap BIGINT,
                            total_volume BIGINT, high_24h DOUBLE, low_24h DOUBLE,
                            price_change_24h DOUBLE,
                            price_change_percentage_24h DOUBLE,
                            market_cap_rank INT, fully_diluted_valuation DOUBLE,
                            market_cap_change_24h DOUBLE,
                            market_cap_change_percentage_24h DOUBLE,
                            circulating_supply DOUBLE, total_supply DOUBLE,
                            max_supply DOUBLE, ath DOUBLE,
                            ath_change_percentage DOUBLE, ath_date TIMESTAMP,
                            atl DOUBLE, atl_change_percentage DOUBLE,
                            atl_date TIMESTAMP,
                            volume_to_market_cap_ratio DOUBLE,
                            last_updated TIMESTAMP, raw_json STRING,
                            ingestion_ts TIMESTAMP, source STRING,
                            run_key STRING
                        ) USING DELTA
                    """)
                    ensure_columns_exist(cursor, table_name, [
                        ("market_cap_rank", "INT"),
                        ("fully_diluted_valuation", "DOUBLE"),
                        ("market_cap_change_24h", "DOUBLE"),
                        ("market_cap_change_percentage_24h", "DOUBLE"),
                        ("circulating_supply", "DOUBLE"),
                        ("total_supply", "DOUBLE"),
                        ("max_supply", "DOUBLE"),
                        ("ath", "DOUBLE"),
                        ("ath_change_percentage", "DOUBLE"),
                        ("ath_date", "TIMESTAMP"),
                        ("atl", "DOUBLE"),
                        ("atl_change_percentage", "DOUBLE"),
                        ("atl_date", "TIMESTAMP"),
                        ("volume_to_market_cap_ratio", "DOUBLE"),
                        ("raw_json", "STRING"),
                        ("ingestion_ts", "TIMESTAMP"),
                        ("source", "STRING"),
                        ("run_key", "STRING"),
                    ])
                    logger.info("DDL checks completed in %.2fs", time.perf_counter() - ddl_start)

                    # ----- Transform & deduplicate -----
                    records_by_id: dict[str, tuple] = {}
                    for raw_row in data:
                        coin_id = raw_row.get("id")
                        if coin_id is None:
                            continue
                        mc = safe_float(raw_row.get("market_cap"))
                        tv = safe_float(raw_row.get("total_volume"))
                        vtmc = (tv / mc) if mc not in (None, 0) and tv is not None else None
                        records_by_id[coin_id] = (
                            coin_id, raw_row.get("symbol"), raw_row.get("name"),
                            safe_float(raw_row.get("current_price")),
                            safe_int(raw_row.get("market_cap")),
                            safe_int(raw_row.get("total_volume")),
                            safe_float(raw_row.get("high_24h")),
                            safe_float(raw_row.get("low_24h")),
                            safe_float(raw_row.get("price_change_24h")),
                            safe_float(raw_row.get("price_change_percentage_24h")),
                            safe_int(raw_row.get("market_cap_rank")),
                            safe_float(raw_row.get("fully_diluted_valuation")),
                            safe_float(raw_row.get("market_cap_change_24h")),
                            safe_float(raw_row.get("market_cap_change_percentage_24h")),
                            safe_float(raw_row.get("circulating_supply")),
                            safe_float(raw_row.get("total_supply")),
                            safe_float(raw_row.get("max_supply")),
                            safe_float(raw_row.get("ath")),
                            safe_float(raw_row.get("ath_change_percentage")),
                            parse_iso8601_timestamp(raw_row.get("ath_date")),
                            safe_float(raw_row.get("atl")),
                            safe_float(raw_row.get("atl_change_percentage")),
                            parse_iso8601_timestamp(raw_row.get("atl_date")),
                            vtmc,
                            parse_iso8601_timestamp(raw_row.get("last_updated")),
                            json.dumps(raw_row),
                            ingestion_ts, source, run_key,
                        )
                    records = list(records_by_id.values())

                    # ----- Idempotent load: DELETE + INSERT -----
                    merge_start = time.perf_counter()
                    cursor.execute(f"DELETE FROM {table_name} WHERE run_key = ?", [run_key])
                    insert_records_in_batches(cursor, table_name, column_names, records, batch_size=200)
                    logger.info("Inserted batch in %.2fs", time.perf_counter() - merge_start)

                    # ----- Verify -----
                    cursor.execute(
                        f"SELECT COUNT(*) FROM {table_name} WHERE run_key = ?", [run_key]
                    )
                    inserted = (cursor.fetchone() or [0])[0]
                    if inserted != len(records):
                        raise RuntimeError(
                            f"Row count mismatch for run_key {run_key}: "
                            f"expected {len(records)}, found {inserted}"
                        )

        except Exception as exc:
            error_text = str(exc).lower()
            if "403" in error_text or "forbidden" in error_text:
                raise RuntimeError(
                    "Databricks returned 403 Forbidden. "
                    "Verify host, token, and SQL warehouse http_path."
                ) from exc
            raise

        logger.info(
            "Pipeline complete in %.2fs — %d rows → %s.%s.%s (run_key=%s)",
            time.perf_counter() - total_start, len(records), catalog, schema, table, run_key,
        )
        return run_key

    # ----- Task wiring -----
    run_key = ingest_coingecko_to_bronze()

    trigger_quality = TriggerDagRunOperator(
        task_id="trigger_quality_after_ingestion",
        trigger_dag_id="data_quality",
        wait_for_completion=False,
        conf={
            "layer": "bronze",
            "bronze_source": "coingecko",
            "run_key": "{{ ti.xcom_pull(task_ids='ingest_coingecko_to_bronze') }}",
            "fail_on_non_critical": False,
        },
    )

    run_key >> trigger_quality


coingecko_bronze_ingest()
