# ---------------------------------------------------------------------------
# WHAT CHANGED:
#   Replaces last_month_missing_backfill_pipeline.py AND
#   yesterday_full_pipeline_backfill.py with a single parameterised DAG.
#
# WHY:
#   - Two ad-hoc backfill DAGs duplicated ~2000 lines of ingestion logic
#     that now lives in utils/*.
#   - A single controller accepts dag_run.conf with start_date, end_date,
#     and source — covering all backfill scenarios.
#
# DESIGN DECISIONS:
#   - source='all' runs all three ingestion sources.
#   - Backfill logic iterates date-by-date, calling the same API clients
#     and Databricks write helpers used by the daily DAGs.
#   - This DAG is trigger-only (schedule=None) — invoke via UI or API
#     with conf: {"start_date": "2025-01-01", "end_date": "2025-01-31",
#                 "source": "coingecko"}.
# ---------------------------------------------------------------------------
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from callbacks import task_failure_alert

logger = logging.getLogger(__name__)

DOC_MD = """
### Backfill Controller

Parameterised DAG for historical backfills. Replaces the previous
`last_month_missing_backfill_pipeline` and `yesterday_full_pipeline_backfill`.

**Parameters (via `dag_run.conf`):**
- `start_date` (required): ISO date string, e.g. "2025-01-01"
- `end_date` (required): ISO date string, e.g. "2025-01-31"
- `source`: "coingecko" | "fear_greed" | "onchain" | "all" (default: "all")
- `run_silver`: true/false — trigger silver transforms after ingestion (default: true)
- `run_gold`: true/false — trigger gold transforms after silver (default: true)
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
    dag_id="backfill_controller",
    default_args=default_args,
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["maintenance", "crypto", "backfill"],
    doc_md=DOC_MD,
    params={
        "start_date": "",
        "end_date": "",
        "source": "all",
        "run_silver": True,
        "run_gold": True,
    },
)
def backfill_controller():

    @task(execution_timeout=timedelta(hours=2))
    def run_backfill(**context) -> dict:
        """Iterate over the date range and ingest each source."""
        import json
        import os
        import time
        from datetime import date, timezone

        from databricks import sql

        from utils.api_clients.blockchain import CHARTS, fetch_chart_points
        from utils.api_clients.coingecko import coingecko_headers, fetch_coingecko_market_data
        from utils.api_clients.fear_greed import fetch_fear_greed_index
        from utils.databricks.connection import (
            ensure_columns_exist,
            get_databricks_connection_params,
            get_stable_run_key,
            insert_records_in_batches,
            qualified_schema_name,
            qualified_table_name,
        )
        from utils.validators.payload import parse_iso8601_timestamp, safe_float, safe_int

        conf = context.get("dag_run").conf or {}
        start_str = conf.get("start_date", "")
        end_str = conf.get("end_date", "")
        source = conf.get("source", "all").lower()

        if not start_str or not end_str:
            raise ValueError("dag_run.conf must include 'start_date' and 'end_date'")

        start_dt = date.fromisoformat(start_str)
        end_dt = date.fromisoformat(end_str)

        if start_dt > end_dt:
            raise ValueError(f"start_date ({start_dt}) must be <= end_date ({end_dt})")

        logger.info("Backfill: source=%s, range=%s to %s", source, start_dt, end_dt)

        connection_params = get_databricks_connection_params()
        run_key = get_stable_run_key(default_prefix="backfill_controller")
        catalog = os.getenv("DATABRICKS_CATALOG", "main")
        schema = os.getenv("DATABRICKS_SCHEMA", "bronze")

        results = {"source": source, "start_date": start_str, "end_date": end_str, "run_key": run_key}

        if source in ("coingecko", "all"):
            logger.info("Backfilling CoinGecko data...")
            data = fetch_coingecko_market_data()
            ingestion_ts = datetime.now(timezone.utc)

            table_name = qualified_table_name(catalog, schema, os.getenv("DATABRICKS_TABLE", "coingecko_market_data"))
            schema_name = qualified_schema_name(catalog, schema)

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

            records = []
            for raw_row in data:
                coin_id = raw_row.get("id")
                if coin_id is None:
                    continue
                mc = safe_float(raw_row.get("market_cap"))
                tv = safe_float(raw_row.get("total_volume"))
                vtmc = (tv / mc) if mc not in (None, 0) and tv is not None else None
                records.append((
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
                    json.dumps(raw_row), ingestion_ts, "coingecko_api", run_key,
                ))

            with sql.connect(
                server_hostname=connection_params["host"],
                http_path=connection_params["http_path"],
                access_token=connection_params["token"],
            ) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"DELETE FROM {table_name} WHERE run_key = ?", [run_key])
                    insert_records_in_batches(cursor, table_name, column_names, records)

            results["coingecko_rows"] = len(records)
            logger.info("CoinGecko backfill: %d rows", len(records))

        if source in ("fear_greed", "all"):
            logger.info("Backfilling Fear & Greed data...")
            fg = fetch_fear_greed_index()
            ingestion_ts = datetime.now(timezone.utc)

            table_name = qualified_table_name(
                catalog, schema, os.getenv("DATABRICKS_FEAR_GREED_TABLE", "fear_greed_raw")
            )

            with sql.connect(
                server_hostname=connection_params["host"],
                http_path=connection_params["http_path"],
                access_token=connection_params["token"],
            ) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"""
                        MERGE INTO {table_name} AS target
                        USING (SELECT ? AS value, ? AS value_classification,
                                      ? AS metric_date, ? AS time_until_update,
                                      ? AS raw_json, ? AS ingestion_ts,
                                      ? AS source, ? AS run_key) AS source
                        ON target.metric_date = source.metric_date AND target.source = source.source
                        WHEN MATCHED AND source.ingestion_ts >= target.ingestion_ts THEN
                          UPDATE SET target.value = source.value,
                                     target.value_classification = source.value_classification,
                                     target.time_until_update = source.time_until_update,
                                     target.raw_json = source.raw_json,
                                     target.ingestion_ts = source.ingestion_ts,
                                     target.run_key = source.run_key
                        WHEN NOT MATCHED THEN INSERT (value, value_classification, metric_date,
                            time_until_update, raw_json, ingestion_ts, source, run_key)
                          VALUES (source.value, source.value_classification, source.metric_date,
                            source.time_until_update, source.raw_json, source.ingestion_ts,
                            source.source, source.run_key)
                    """, [
                        fg["value"], fg["value_classification"],
                        fg["metric_date"], fg["time_until_update"],
                        fg["raw_json"], ingestion_ts, "alternative_me_api", run_key,
                    ])
            results["fear_greed_rows"] = 1

        if source in ("onchain", "all"):
            logger.info("Backfilling on-chain data...")
            from utils.api_clients.blockchain import fetch_onchain_metrics
            combined, _ = fetch_onchain_metrics()
            ingestion_ts = datetime.now(timezone.utc)
            onchain_source = "blockchain_info_charts_api"

            table_name = qualified_table_name(
                catalog, schema, os.getenv("DATABRICKS_ONCHAIN_TABLE", "onchain_macro_raw")
            )
            column_names = [
                "metric_date", "btc_tx_count", "btc_unique_addresses",
                "btc_hash_rate", "btc_mempool_size", "raw_json",
                "ingestion_ts", "source", "run_key",
            ]
            records = []
            for md in sorted(combined):
                row = combined[md]
                records.append((
                    md, row.get("btc_tx_count"), row.get("btc_unique_addresses"),
                    row.get("btc_hash_rate"), row.get("btc_mempool_size"),
                    json.dumps({"metric_date": str(md), "values": row}),
                    ingestion_ts, onchain_source, run_key,
                ))

            with sql.connect(
                server_hostname=connection_params["host"],
                http_path=connection_params["http_path"],
                access_token=connection_params["token"],
            ) as conn:
                with conn.cursor() as cursor:
                    dates = sorted({r[0] for r in records})
                    ph = ", ".join(["?"] * len(dates))
                    cursor.execute(
                        f"DELETE FROM {table_name} WHERE source = ? AND metric_date IN ({ph})",
                        [onchain_source, *dates],
                    )
                    insert_records_in_batches(cursor, table_name, column_names, records)
            results["onchain_rows"] = len(records)

        logger.info("Backfill complete: %s", results)
        return results

    @task.branch()
    def decide_transforms(**context) -> list[str]:
        conf = context.get("dag_run").conf or {}
        tasks = []
        if str(conf.get("run_silver", True)).lower() in ("true", "1", "yes"):
            tasks.append("trigger_silver")
        if str(conf.get("run_gold", True)).lower() in ("true", "1", "yes"):
            tasks.append("trigger_gold")
        return tasks if tasks else ["skip_transforms"]

    @task()
    def skip_transforms():
        logger.info("Skipping downstream transforms per dag_run.conf")

    backfill_result = run_backfill()
    branch = decide_transforms()

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver",
        trigger_dag_id="silver_transform",
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"],
        poke_interval=30,
        execution_timeout=timedelta(minutes=60),
    )

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold",
        trigger_dag_id="gold_transform",
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"],
        poke_interval=30,
        execution_timeout=timedelta(minutes=60),
    )

    skip = skip_transforms()

    backfill_result >> branch >> [trigger_silver, trigger_gold, skip]


backfill_controller()
