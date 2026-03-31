# ---------------------------------------------------------------------------
# WHAT CHANGED:
#   Extracted all Databricks connection and SQL helper functions that were
#   duplicated across coingecko_pipeline.py, fear_greed_pipeline.py,
#   blockchain_onchain_pipeline.py, and both backfill DAGs into a single
#   shared module.
#
# WHY:
#   - Eliminates ~120 lines of duplicated connection/SQL code per DAG file.
#   - Single place to update connection logic, placeholder detection, or
#     identifier quoting rules.
#   - All DAGs now import from utils.databricks.connection.
# ---------------------------------------------------------------------------
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Optional, Sequence
from urllib.parse import urlparse

from airflow.sdk.bases.hook import BaseHook


def quote_identifier(identifier: str) -> str:
    """Safely quote a Databricks SQL identifier (handles backticks and hyphens)."""
    if identifier is None:
        raise ValueError("Identifier cannot be None")
    value = str(identifier).strip()
    if not value:
        raise ValueError("Identifier cannot be empty")
    return f"`{value.replace('`', '``')}`"


def qualified_table_name(catalog: str, schema: str, table: str) -> str:
    return ".".join([quote_identifier(catalog), quote_identifier(schema), quote_identifier(table)])


def qualified_schema_name(catalog: str, schema: str) -> str:
    return ".".join([quote_identifier(catalog), quote_identifier(schema)])


def chunk_records(records: list, batch_size: int):
    """Yield successive chunks from *records*."""
    for start in range(0, len(records), batch_size):
        yield records[start : start + batch_size]


def ensure_columns_exist(cursor, table_name: str, columns_with_types: Sequence[tuple[str, str]]):
    """Idempotent ALTER TABLE ADD COLUMNS — ignores 'already exists' errors."""
    for column_name, column_type in columns_with_types:
        try:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMNS ({column_name} {column_type})")
        except Exception as exc:
            if "already exists" not in str(exc).lower():
                raise


def insert_records_in_batches(
    cursor,
    table_name: str,
    column_names: list[str],
    records: list[tuple],
    batch_size: int = 200,
):
    """Batch-INSERT rows into *table_name*. Column list is explicit to avoid SQL injection."""
    if not records:
        return

    row_placeholder = "(" + ", ".join(["?"] * len(column_names)) + ")"
    insert_columns_sql = ", ".join(column_names)

    for batch in chunk_records(records, batch_size):
        values_sql = ", ".join([row_placeholder] * len(batch))
        insert_sql = f"INSERT INTO {table_name} ({insert_columns_sql}) VALUES {values_sql}"
        flat_params = [value for row in batch for value in row]
        cursor.execute(insert_sql, flat_params)


def _is_placeholder(value: Optional[str]) -> bool:
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


def get_databricks_connection_params() -> dict[str, str]:
    """Resolve Databricks host/token/http_path from the Airflow connection + env vars.

    Raises ValueError with actionable messages on missing or placeholder credentials.
    """
    conn = BaseHook.get_connection("databricks_default")
    extras = conn.extra_dejson or {}

    def _clean(value: Any) -> Optional[str]:
        return value.strip() if isinstance(value, str) else value

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
            "DATABRICKS_HTTP_PATH is still a placeholder. "
            "Set it to your real SQL warehouse path, e.g. /sql/1.0/warehouses/<warehouse_id>."
        )
    if _is_placeholder(token):
        raise ValueError(
            "Databricks token looks like a placeholder. "
            "Set a valid PAT in connection 'databricks_default' or DATABRICKS_TOKEN."
        )

    return {"host": host, "token": token, "http_path": http_path}


def get_stable_run_key(default_prefix: str) -> str:
    """Build a deterministic run_key from Airflow context env vars.

    When running inside an Airflow task the key is ``<dag_id>:<dag_run_id>``.
    Outside a task context (local debugging) a UTC-timestamped fallback is used.
    """
    dag_run_id = (os.getenv("AIRFLOW_CTX_DAG_RUN_ID") or "").strip()
    dag_id = (os.getenv("AIRFLOW_CTX_DAG_ID") or default_prefix).strip() or default_prefix

    if dag_run_id:
        return f"{dag_id}:{dag_run_id}"

    generated_suffix = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{dag_id}:manual:{generated_suffix}"
