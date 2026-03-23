from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk.bases.hook import BaseHook
from datetime import datetime, timezone
import requests
import pandas as pd
import os
import json
import time
import uuid
from urllib.parse import urlparse
from databricks import sql


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


def _insert_records_in_batches(cursor, table_name, records, batch_size=200):
    if not records:
        return

    columns_sql = (
        "(id, symbol, name, current_price, market_cap, total_volume, high_24h, low_24h, "
        "price_change_24h, price_change_percentage_24h, last_updated, raw_json, ingestion_ts, source, batch_id)"
    )

    row_placeholder = "(" + ", ".join(["?"] * 15) + ")"

    for batch in _chunk_records(records, batch_size):
        values_sql = ", ".join([row_placeholder] * len(batch))
        insert_sql = f"INSERT INTO {table_name} {columns_sql} VALUES {values_sql}"
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

    columns_to_keep = [
        "id", "symbol", "name", "current_price", "market_cap",
        "total_volume", "high_24h", "low_24h", "price_change_24h",
        "price_change_percentage_24h", "last_updated"
    ]
    _validate_coingecko_payload(data, columns_to_keep)

    df = pd.DataFrame(data)
    df = df[columns_to_keep]

    ingestion_ts = datetime.now(timezone.utc)
    batch_id = str(uuid.uuid4())
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
                        last_updated TIMESTAMP,
                        raw_json STRING,
                        ingestion_ts TIMESTAMP,
                        source STRING,
                        batch_id STRING
                    )
                    USING DELTA
                    """
                )
                # Ensure backward-compatible schema upgrades on partially upgraded bronze tables.
                _ensure_columns_exist(
                    cursor,
                    table_name,
                    [
                        ("raw_json", "STRING"),
                        ("ingestion_ts", "TIMESTAMP"),
                        ("source", "STRING"),
                        ("batch_id", "STRING"),
                    ],
                )
                print(f"DDL checks completed in {time.perf_counter() - ddl_start:.2f}s")

                records = []
                for row_tuple, raw_row in zip(df.itertuples(index=False, name=None), data):
                    records.append(
                        tuple(row_tuple)
                        + (
                            json.dumps(raw_row),
                            ingestion_ts,
                            source,
                            batch_id,
                        )
                    )
                insert_start = time.perf_counter()
                _insert_records_in_batches(cursor, table_name, records, batch_size=200)
                print(f"Inserted batch in {time.perf_counter() - insert_start:.2f}s")

                cursor.execute(
                    f"SELECT COUNT(*) FROM {table_name} WHERE batch_id = ?",
                    [batch_id],
                )
                inserted_count_row = cursor.fetchone()
                inserted_count = inserted_count_row[0] if inserted_count_row else 0
                if inserted_count != len(records):
                    raise RuntimeError(
                        f"Inserted row count mismatch for batch {batch_id}: "
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
    print(f"Inserted {len(df)} rows into {catalog}.{schema}.{table}")
    print(f"Ingestion batch_id: {batch_id}")
    return batch_id


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 21),
    "retries": 1,
}

with DAG(
    dag_id="coingecko_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
) as dag:

    ingest_bronze_task = PythonOperator(
        task_id="ingest_coingecko_to_bronze",
        python_callable=fetch_coingecko_data,
    )

    trigger_quality_after_ingestion = TriggerDagRunOperator(
        task_id="trigger_quality_after_ingestion",
        trigger_dag_id="coingecko_quality_pipeline",
        conf={
            "layer": "bronze",
            "batch_id": "{{ ti.xcom_pull(task_ids='ingest_coingecko_to_bronze') }}",
            "fail_on_non_critical": False,
        },
    )

    ingest_bronze_task >> trigger_quality_after_ingestion
