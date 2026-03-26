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


CHARTS = {
    "btc_tx_count": "n-transactions",
    "btc_unique_addresses": "n-unique-addresses",
    "btc_hash_rate": "hash-rate",
    "btc_mempool_size": "mempool-size",
}


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


def _insert_records_in_batches(cursor, table_name, records, batch_size=200):
    if not records:
        return

    column_names = [
        "metric_date",
        "btc_tx_count",
        "btc_unique_addresses",
        "btc_hash_rate",
        "btc_mempool_size",
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


def _get_stable_run_key(default_prefix):
    dag_run_id = (os.getenv("AIRFLOW_CTX_DAG_RUN_ID") or "").strip()
    dag_id = (os.getenv("AIRFLOW_CTX_DAG_ID") or default_prefix).strip() or default_prefix

    if dag_run_id:
        return f"{dag_id}:{dag_run_id}"

    # Fallback is only used outside Airflow task context (for local/debug execution).
    generated_suffix = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{dag_id}:manual:{generated_suffix}"


def _fetch_chart_points(chart_name, timespan_days):
    url = f"https://api.blockchain.info/charts/{chart_name}"
    response = requests.get(
        url,
        params={
            "timespan": f"{timespan_days}days",
            "format": "json",
            "sampled": "true",
        },
        timeout=30,
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"Blockchain.com chart request failed for {chart_name} with status {response.status_code}: {response.text[:500]}"
        )

    payload = response.json()
    values = payload.get("values") if isinstance(payload, dict) else None
    if not isinstance(values, list):
        raise RuntimeError(f"Blockchain.com payload for {chart_name} is missing a values array")

    points = {}
    for item in values:
        if not isinstance(item, dict):
            continue
        timestamp = _safe_int(item.get("x"))
        point_value = _safe_float(item.get("y"))
        if timestamp is None or point_value is None:
            continue
        metric_date = datetime.fromtimestamp(timestamp, tz=timezone.utc).date()
        points[metric_date] = point_value

    return payload, points


def fetch_onchain_macro_data():
    total_start = time.perf_counter()
    timespan_days = _safe_int(os.getenv("BLOCKCHAIN_CHART_TIMESPAN_DAYS")) or 120
    source = "blockchain_info_charts_api"
    ingestion_ts = datetime.now(timezone.utc)
    run_key = _get_stable_run_key(default_prefix="ingest_bronze_blockchain_onchain_metrics")

    combined_by_date = {}
    payload_cache = {}

    for metric_name, chart_name in CHARTS.items():
        api_start = time.perf_counter()
        payload, points = _fetch_chart_points(chart_name, timespan_days)
        payload_cache[metric_name] = payload
        print(
            f"Blockchain.com chart {chart_name} fetched in {time.perf_counter() - api_start:.2f}s with {len(points)} points"
        )

        for metric_date, metric_value in points.items():
            combined_by_date.setdefault(metric_date, {})[metric_name] = metric_value

    if not combined_by_date:
        raise RuntimeError("No on-chain rows were returned by Blockchain.com charts API")

    connection_params = _get_databricks_connection_params()
    catalog = os.getenv("DATABRICKS_CATALOG", "main")
    schema = os.getenv("DATABRICKS_SCHEMA", "bronze")
    table = os.getenv("DATABRICKS_ONCHAIN_TABLE", "onchain_macro_raw")
    schema_name = _qualified_schema_name(catalog, schema)
    table_name = _qualified_table_name(catalog, schema, table)

    records = []
    for metric_date in sorted(combined_by_date):
        row = combined_by_date[metric_date]
        row_raw_json = json.dumps(
            {
                "metric_date": str(metric_date),
                "values": row,
            }
        )
        records.append(
            (
                metric_date,
                row.get("btc_tx_count"),
                row.get("btc_unique_addresses"),
                row.get("btc_hash_rate"),
                row.get("btc_mempool_size"),
                row_raw_json,
                ingestion_ts,
                source,
                run_key,
            )
        )

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
                        metric_date DATE,
                        btc_tx_count DOUBLE,
                        btc_unique_addresses DOUBLE,
                        btc_hash_rate DOUBLE,
                        btc_mempool_size DOUBLE,
                        raw_json STRING,
                        ingestion_ts TIMESTAMP,
                        source STRING,
                        run_key STRING
                    )
                    USING DELTA
                    """
                )
                _ensure_columns_exist(
                    cursor,
                    table_name,
                    [
                        ("metric_date", "DATE"),
                        ("btc_tx_count", "DOUBLE"),
                        ("btc_unique_addresses", "DOUBLE"),
                        ("btc_hash_rate", "DOUBLE"),
                        ("btc_mempool_size", "DOUBLE"),
                        ("raw_json", "STRING"),
                        ("ingestion_ts", "TIMESTAMP"),
                        ("source", "STRING"),
                        ("run_key", "STRING"),
                    ],
                )
                print(f"DDL checks completed in {time.perf_counter() - ddl_start:.2f}s")

                insert_start = time.perf_counter()
                metric_dates = sorted({row[0] for row in records})
                date_placeholders = ", ".join(["?"] * len(metric_dates))
                # Fast idempotent load: remove rows for incoming dates+source, then batch insert.
                cursor.execute(
                                        f"""
                                        DELETE FROM {table_name}
                                        WHERE source = ?
                                            AND metric_date IN ({date_placeholders})
                                        """,
                                        [source, *metric_dates],
                )
                _insert_records_in_batches(cursor, table_name, records, batch_size=200)
                print(f"Inserted batch in {time.perf_counter() - insert_start:.2f}s")

    except Exception as exc:
        error_text = str(exc).lower()
        if "403" in error_text or "forbidden" in error_text:
            raise RuntimeError(
                "Databricks returned 403 Forbidden during SQL session open. "
                "Verify host, token permissions, and SQL warehouse http_path in databricks_default / DATABRICKS_HTTP_PATH."
            ) from exc
        raise

    print(f"Total pipeline task runtime: {time.perf_counter() - total_start:.2f}s")
    print(f"Upserted {len(records)} on-chain macro rows")
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
    dag_id="ingest_bronze_blockchain_onchain_metrics",
    default_args=default_args,
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
) as dag:

    ingest_bronze_task = PythonOperator(
        task_id="ingest_onchain_macro_to_bronze",
        python_callable=fetch_onchain_macro_data,
        execution_timeout=timedelta(minutes=20),
    )

    trigger_quality_after_ingestion = TriggerDagRunOperator(
        task_id="trigger_quality_after_ingestion",
        trigger_dag_id="quality_checks_crypto_data_layers",
        # Run quality checks asynchronously to avoid blocking worker slots.
        wait_for_completion=False,
        conf={
            "layer": "bronze",
            "bronze_source": "onchain",
            "run_key": "{{ ti.xcom_pull(task_ids='ingest_onchain_macro_to_bronze') }}",
            "fail_on_non_critical": False,
        },
    )

    ingest_bronze_task >> trigger_quality_after_ingestion
