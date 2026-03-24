from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk.bases.hook import BaseHook
from datetime import datetime, timezone, timedelta
import requests
import os
import time
from urllib.parse import urlparse
from databricks import sql
from pipeline_callbacks import task_failure_alert, sla_miss_alert


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


def fetch_fear_greed_data():
    total_start = time.perf_counter()
    url = "https://api.alternative.me/fng/?limit=1&format=json"

    api_start = time.perf_counter()
    response = requests.get(url, timeout=30)
    if response.status_code != 200:
        raise RuntimeError(
            f"Alternative.me request failed with status {response.status_code}: {response.text[:500]}"
        )
    print(f"Alternative.me API call completed in {time.perf_counter() - api_start:.2f}s")

    raw_response_text = response.text
    payload = response.json()

    metadata = payload.get("metadata") if isinstance(payload, dict) else None
    metadata_error = metadata.get("error") if isinstance(metadata, dict) else None
    if metadata_error is not None:
        raise RuntimeError(f"Alternative.me returned metadata.error: {metadata_error}")

    data_points = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data_points, list) or not data_points:
        raise RuntimeError("Alternative.me payload is missing a non-empty data array")

    latest = data_points[0]
    if not isinstance(latest, dict):
        raise RuntimeError("Alternative.me payload data[0] is not a JSON object")

    value = _safe_int(latest.get("value"))
    value_classification = latest.get("value_classification")
    metric_timestamp = _safe_int(latest.get("timestamp"))
    # The API timestamp is Unix seconds; convert in UTC and persist date only.
    metric_date = (
        datetime.fromtimestamp(metric_timestamp, tz=timezone.utc).date()
        if metric_timestamp is not None
        else None
    )
    time_until_update = _safe_int(latest.get("time_until_update"))

    if value is None:
        raise RuntimeError("Alternative.me payload is missing a valid integer 'value'")
    if value_classification is None:
        raise RuntimeError("Alternative.me payload is missing 'value_classification'")
    if metric_date is None:
        raise RuntimeError("Alternative.me payload is missing a valid Unix 'timestamp'")

    ingestion_ts = datetime.now(timezone.utc)
    run_key = _get_stable_run_key(default_prefix="fear_greed_pipeline")
    source = "alternative_me_api"

    connection_params = _get_databricks_connection_params()
    catalog = os.getenv("DATABRICKS_CATALOG", "crypto-data-pipeline")
    schema = os.getenv("DATABRICKS_SCHEMA", "bronze")
    table = os.getenv("DATABRICKS_FEAR_GREED_TABLE", "fear_greed_raw")
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
                        value INT,
                        value_classification STRING,
                        metric_date DATE,
                        time_until_update INT,
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
                        ("value", "INT"),
                        ("value_classification", "STRING"),
                        ("metric_date", "DATE"),
                        ("time_until_update", "INT"),
                        ("raw_json", "STRING"),
                        ("ingestion_ts", "TIMESTAMP"),
                        ("source", "STRING"),
                        ("run_key", "STRING"),
                    ],
                )
                print(f"DDL checks completed in {time.perf_counter() - ddl_start:.2f}s")

                merge_start = time.perf_counter()
                cursor.execute(
                    f"""
                    MERGE INTO {table_name} AS target
                    USING (
                        SELECT
                            ? AS value,
                            ? AS value_classification,
                            ? AS metric_date,
                            ? AS time_until_update,
                            ? AS raw_json,
                            ? AS ingestion_ts,
                            ? AS source,
                            ? AS run_key
                    ) AS source
                                        ON target.metric_date = source.metric_date AND target.source = source.source
                                        WHEN MATCHED AND source.ingestion_ts >= target.ingestion_ts THEN
                      UPDATE SET
                        target.value = source.value,
                        target.value_classification = source.value_classification,
                        target.time_until_update = source.time_until_update,
                        target.raw_json = source.raw_json,
                        target.ingestion_ts = source.ingestion_ts,
                        target.source = source.source,
                        target.run_key = source.run_key
                    WHEN NOT MATCHED THEN
                      INSERT (
                        value,
                        value_classification,
                        metric_date,
                        time_until_update,
                        raw_json,
                        ingestion_ts,
                        source,
                        run_key
                      )
                      VALUES (
                        source.value,
                        source.value_classification,
                        source.metric_date,
                        source.time_until_update,
                        source.raw_json,
                        source.ingestion_ts,
                        source.source,
                        source.run_key
                      )
                    """,
                    [
                        value,
                        value_classification,
                        metric_date,
                        time_until_update,
                        raw_response_text,
                        ingestion_ts,
                        source,
                        run_key,
                    ],
                )
                print(f"Merged row in {time.perf_counter() - merge_start:.2f}s")

    except Exception as exc:
        error_text = str(exc).lower()
        if "403" in error_text or "forbidden" in error_text:
            raise RuntimeError(
                "Databricks returned 403 Forbidden during SQL session open. "
                "Verify host, token permissions, and SQL warehouse http_path in databricks_default / DATABRICKS_HTTP_PATH."
            ) from exc
        raise

    print(f"Total pipeline task runtime: {time.perf_counter() - total_start:.2f}s")
    print(f"Upserted Fear & Greed value={value}, classification={value_classification}")
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
    "sla": timedelta(hours=1),
}

with DAG(
    dag_id="fear_greed_pipeline",
    default_args=default_args,
    schedule=None,
    catchup=False,
    sla_miss_callback=sla_miss_alert,
) as dag:

    ingest_bronze_task = PythonOperator(
        task_id="ingest_fear_greed_to_bronze",
        python_callable=fetch_fear_greed_data,
        execution_timeout=timedelta(minutes=15),
    )

    trigger_quality_after_ingestion = TriggerDagRunOperator(
        task_id="trigger_quality_after_ingestion",
        trigger_dag_id="crypto_data_quality_pipeline",
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
        conf={
            "layer": "bronze",
            "run_key": "{{ ti.xcom_pull(task_ids='ingest_fear_greed_to_bronze') }}",
            "fail_on_non_critical": False,
        },
    )

    ingest_bronze_task >> trigger_quality_after_ingestion
