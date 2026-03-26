from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk.bases.hook import BaseHook
from datetime import datetime, timezone, timedelta
import requests
import os
import json
import time
from urllib.parse import urlparse
from databricks import sql
from pipeline_callbacks import task_failure_alert


def _quote_identifier(identifier):
    """Safely quote Databricks SQL identifiers, including names with '-' characters."""
    if identifier is None:
        raise ValueError("Identifier cannot be None")
    value = str(identifier).strip()
    if not value:
        raise ValueError("Identifier cannot be empty")
    return f"`{value.replace('`', '``')}`"


def _qualified_table_name(catalog, schema, table):
    return ".".join(
        [
            _quote_identifier(catalog),
            _quote_identifier(schema),
            _quote_identifier(table),
        ]
    )


def _qualified_schema_name(catalog, schema):
    return ".".join([_quote_identifier(catalog), _quote_identifier(schema)])


def _chunk_records(records, batch_size):
    for start in range(0, len(records), batch_size):
        yield records[start : start + batch_size]


def _validate_coingecko_payload(data, required_columns):
    if not isinstance(data, list):
        raise ValueError("CoinGecko payload must be a list of coin records")
    if not data:
        raise ValueError("CoinGecko payload is empty")

    missing_columns = set()
    for index, row in enumerate(data):
        if not isinstance(row, dict):
            raise ValueError(f"CoinGecko payload row {index} is not a JSON object")
        for column in required_columns:
            if column not in row:
                missing_columns.add(column)

    if missing_columns:
        raise ValueError(
            "CoinGecko payload is missing expected fields: "
            + ", ".join(sorted(missing_columns))
        )


def _safe_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value):
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_iso8601_timestamp(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        normalized_value = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized_value)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return None


def _get_stable_run_key(default_prefix):
    dag_run_id = (os.getenv("AIRFLOW_CTX_DAG_RUN_ID") or "").strip()
    dag_id = (os.getenv("AIRFLOW_CTX_DAG_ID") or default_prefix).strip() or default_prefix

    if dag_run_id:
        return f"{dag_id}:{dag_run_id}"

    # Fallback is only used outside Airflow task context (for local/debug execution).
    generated_suffix = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{dag_id}:manual:{generated_suffix}"


def _insert_records_in_batches(cursor, table_name, records, batch_size=200):
    if not records:
        return

    column_names = [
        "id",
        "symbol",
        "name",
        "current_price",
        "market_cap",
        "total_volume",
        "high_24h",
        "low_24h",
        "price_change_24h",
        "price_change_percentage_24h",
        "market_cap_rank",
        "fully_diluted_valuation",
        "market_cap_change_24h",
        "market_cap_change_percentage_24h",
        "circulating_supply",
        "total_supply",
        "max_supply",
        "ath",
        "ath_change_percentage",
        "ath_date",
        "atl",
        "atl_change_percentage",
        "atl_date",
        "volume_to_market_cap_ratio",
        "last_updated",
        "raw_json",
        "ingestion_ts",
        "source",
        "run_key",
    ]

    row_placeholder = "(" + ", ".join(["?"] * len(column_names)) + ")"
    insert_columns_sql = ", ".join(column_names)

    for batch in _chunk_records(records, batch_size):
        values_sql = ", ".join([row_placeholder] * len(batch))
        insert_sql = f"""
            INSERT INTO {table_name} ({insert_columns_sql})
            VALUES {values_sql}
        """
        flat_params = [value for row in batch for value in row]
        cursor.execute(insert_sql, flat_params)


def _ensure_columns_exist(cursor, table_name, columns_with_types):
    for column_name, column_type in columns_with_types:
        try:
            cursor.execute(
                f"""
                ALTER TABLE {table_name}
                ADD COLUMNS ({column_name} {column_type})
                """
            )
        except Exception as exc:
            if "already exists" not in str(exc).lower():
                raise


def _get_databricks_connection_params():
    conn = BaseHook.get_connection("databricks_default")
    extras = conn.extra_dejson or {}

    def _clean(value):
        if isinstance(value, str):
            return value.strip()
        return value

    def _is_placeholder(value):
        if not value:
            return False
        value_lower = value.lower()
        return (
            "replace_me" in value_lower
            or "replace-me" in value_lower
            or "<" in value
            or ">" in value
            or "databricks_pat" in value_lower
            or "your_" in value_lower
            or value_lower in {"changeme", "change-me", "placeholder"}
        )

    host = _clean(conn.host or extras.get("host") or os.getenv("DATABRICKS_HOST"))
    token = _clean(conn.password or extras.get("token") or os.getenv("DATABRICKS_TOKEN"))
    http_path = _clean(extras.get("http_path") or os.getenv("DATABRICKS_HTTP_PATH"))

    if host and "://" in host:
        parsed_host = urlparse(host).hostname
        host = parsed_host or host

    if not host:
        raise ValueError("Missing Databricks host in connection 'databricks_default'")
    if not token:
        raise ValueError("Missing Databricks token in connection 'databricks_default'")
    if not http_path:
        raise ValueError(
            "Missing Databricks SQL warehouse http_path in connection extras or DATABRICKS_HTTP_PATH env"
        )
    if _is_placeholder(http_path):
        raise ValueError(
            "DATABRICKS_HTTP_PATH is still a placeholder. Set it to your real SQL warehouse path, e.g. /sql/1.0/warehouses/<warehouse_id>."
        )
    if _is_placeholder(token):
        raise ValueError(
            "Databricks token looks like a placeholder. Set a valid PAT in connection 'databricks_default' or DATABRICKS_TOKEN."
        )

    return {
        "host": host,
        "token": token,
        "http_path": http_path,
    }

def fetch_coingecko_data():
    total_start = time.perf_counter()
    url = "https://api.coingecko.com/api/v3/coins/markets"

    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 50,
        "page": 1,
        "sparkline": False
    }

    api_start = time.perf_counter()
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    print(f"CoinGecko API call completed in {time.perf_counter() - api_start:.2f}s")

    data = response.json()

    required_columns = [
        "id", "symbol", "name", "current_price", "market_cap",
        "total_volume", "high_24h", "low_24h", "price_change_24h",
        "price_change_percentage_24h", "last_updated"
    ]
    _validate_coingecko_payload(data, required_columns)

    ingestion_ts = datetime.now(timezone.utc)
    run_key = _get_stable_run_key(default_prefix="ingest_bronze_coingecko_market_data")
    source = "coingecko_api"

    connection_params = _get_databricks_connection_params()
    catalog = os.getenv("DATABRICKS_CATALOG", "main")
    schema = os.getenv("DATABRICKS_SCHEMA", "bronze")
    table = os.getenv("DATABRICKS_TABLE", "coingecko_market_data")
    schema_name = _qualified_schema_name(catalog, schema)
    table_name = _qualified_table_name(catalog, schema, table)

    try:
        db_connect_start = time.perf_counter()
        with sql.connect(
            server_hostname=connection_params["host"],
            http_path=connection_params["http_path"],
            access_token=connection_params["token"],
        ) as connection:
            print(f"Opened Databricks SQL connection in {time.perf_counter() - db_connect_start:.2f}s")
            with connection.cursor() as cursor:
                ddl_start = time.perf_counter()
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id STRING,
                        symbol STRING,
                        name STRING,
                        current_price DOUBLE,
                        market_cap BIGINT,
                        total_volume BIGINT,
                        high_24h DOUBLE,
                        low_24h DOUBLE,
                        price_change_24h DOUBLE,
                        price_change_percentage_24h DOUBLE,
                        market_cap_rank INT,
                        fully_diluted_valuation DOUBLE,
                        market_cap_change_24h DOUBLE,
                        market_cap_change_percentage_24h DOUBLE,
                        circulating_supply DOUBLE,
                        total_supply DOUBLE,
                        max_supply DOUBLE,
                        ath DOUBLE,
                        ath_change_percentage DOUBLE,
                        ath_date TIMESTAMP,
                        atl DOUBLE,
                        atl_change_percentage DOUBLE,
                        atl_date TIMESTAMP,
                        volume_to_market_cap_ratio DOUBLE,
                        last_updated TIMESTAMP,
                        raw_json STRING,
                        ingestion_ts TIMESTAMP,
                        source STRING,
                        run_key STRING
                    )
                    USING DELTA
                    """
                )
                # Ensure backward-compatible schema upgrades on partially upgraded bronze tables.
                _ensure_columns_exist(
                    cursor,
                    table_name,
                    [
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
                    ],
                )
                print(f"DDL checks completed in {time.perf_counter() - ddl_start:.2f}s")

                records_by_id = {}
                for raw_row in data:
                    market_cap_value = _safe_float(raw_row.get("market_cap"))
                    total_volume_value = _safe_float(raw_row.get("total_volume"))
                    # The ratio is undefined when market cap is null/zero, so we persist NULL.
                    volume_to_market_cap_ratio = (
                        (total_volume_value / market_cap_value)
                        if market_cap_value not in (None, 0)
                        and total_volume_value is not None
                        else None
                    )
                    coin_id = raw_row.get("id")
                    if coin_id is None:
                        continue
                    records_by_id[coin_id] = (
                        coin_id,
                        raw_row.get("symbol"),
                        raw_row.get("name"),
                        _safe_float(raw_row.get("current_price")),
                        _safe_int(raw_row.get("market_cap")),
                        _safe_int(raw_row.get("total_volume")),
                        _safe_float(raw_row.get("high_24h")),
                        _safe_float(raw_row.get("low_24h")),
                        _safe_float(raw_row.get("price_change_24h")),
                        _safe_float(raw_row.get("price_change_percentage_24h")),
                        _safe_int(raw_row.get("market_cap_rank")),
                        _safe_float(raw_row.get("fully_diluted_valuation")),
                        _safe_float(raw_row.get("market_cap_change_24h")),
                        _safe_float(raw_row.get("market_cap_change_percentage_24h")),
                        _safe_float(raw_row.get("circulating_supply")),
                        _safe_float(raw_row.get("total_supply")),
                        _safe_float(raw_row.get("max_supply")),
                        _safe_float(raw_row.get("ath")),
                        _safe_float(raw_row.get("ath_change_percentage")),
                        _parse_iso8601_timestamp(raw_row.get("ath_date")),
                        _safe_float(raw_row.get("atl")),
                        _safe_float(raw_row.get("atl_change_percentage")),
                        _parse_iso8601_timestamp(raw_row.get("atl_date")),
                        volume_to_market_cap_ratio,
                        _parse_iso8601_timestamp(raw_row.get("last_updated")),
                        json.dumps(raw_row),
                        ingestion_ts,
                        source,
                        run_key,
                    )
                records = list(records_by_id.values())
                merge_start = time.perf_counter()
                # Retry-safe load: remove any previously written rows for this run_key,
                # then insert the current run payload in batches.
                cursor.execute(
                    f"DELETE FROM {table_name} WHERE run_key = ?",
                    [run_key],
                )
                _insert_records_in_batches(cursor, table_name, records, batch_size=200)
                print(f"Inserted batch in {time.perf_counter() - merge_start:.2f}s")

                cursor.execute(
                    f"SELECT COUNT(*) FROM {table_name} WHERE run_key = ?",
                    [run_key],
                )
                inserted_count_row = cursor.fetchone()
                inserted_count = inserted_count_row[0] if inserted_count_row else 0
                if inserted_count != len(records):
                    raise RuntimeError(
                        f"Merged row count mismatch for run_key {run_key}: "
                        f"expected {len(records)}, found {inserted_count}"
                    )
    except Exception as exc:
        error_text = str(exc).lower()
        if "403" in error_text or "forbidden" in error_text:
            raise RuntimeError(
                "Databricks returned 403 Forbidden during SQL session open. "
                "Verify host, token permissions, and SQL warehouse http_path in databricks_default / DATABRICKS_HTTP_PATH."
            ) from exc
        raise

    print(f"Total pipeline task runtime: {time.perf_counter() - total_start:.2f}s")
    print(f"Inserted {len(records)} rows into {catalog}.{schema}.{table}")
    print(f"Ingestion run_key: {run_key}")
    return run_key


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 21),
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "on_failure_callback": task_failure_alert,
}

with DAG(
    dag_id="ingest_bronze_coingecko_market_data",
    default_args=default_args,
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
) as dag:

    ingest_bronze_task = PythonOperator(
        task_id="ingest_coingecko_to_bronze",
        python_callable=fetch_coingecko_data,
        execution_timeout=timedelta(minutes=15),
    )

    trigger_quality_after_ingestion = TriggerDagRunOperator(
        task_id="trigger_quality_after_ingestion",
        trigger_dag_id="quality_checks_crypto_data_layers",
        # Run quality checks asynchronously to avoid blocking worker slots.
        wait_for_completion=False,
        conf={
            "layer": "bronze",
            "bronze_source": "coingecko",
            "run_key": "{{ ti.xcom_pull(task_ids='ingest_coingecko_to_bronze') }}",
            "fail_on_non_critical": False,
        },
    )

    ingest_bronze_task >> trigger_quality_after_ingestion
