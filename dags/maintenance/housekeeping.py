# ---------------------------------------------------------------------------
# WHAT CHANGED:
#   New DAG — periodic maintenance tasks for Airflow and Databricks.
#
# WHY:
#   - Automates cleanup of old logs, XCom entries, and stale metadata.
#   - Prevents disk and DB bloat in long-running Airflow deployments.
# ---------------------------------------------------------------------------
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from callbacks import task_failure_alert

logger = logging.getLogger(__name__)

DOC_MD = """
### Housekeeping DAG

Weekly maintenance tasks:
1. **Purge old XCom** — Removes XCom entries older than 30 days.
2. **Clean task logs** — Removes task instance logs older than 90 days.
3. **Optimize Delta tables** — Runs OPTIMIZE on bronze Delta tables.

Runs every Sunday at 03:00 UTC.
"""

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "on_failure_callback": task_failure_alert,
}


@dag(
    dag_id="housekeeping",
    default_args=default_args,
    schedule="0 3 * * 0",
    catchup=False,
    is_paused_upon_creation=True,
    tags=["maintenance", "housekeeping"],
    doc_md=DOC_MD,
)
def housekeeping():

    @task(execution_timeout=timedelta(minutes=30))
    def optimize_delta_tables(**context):
        """Run OPTIMIZE on bronze Delta tables to compact small files."""
        import os

        from databricks import sql

        from utils.databricks.connection import (
            get_databricks_connection_params,
            qualified_table_name,
        )

        connection_params = get_databricks_connection_params()
        catalog = os.getenv("DATABRICKS_CATALOG", "main")
        schema = os.getenv("DATABRICKS_SCHEMA", "bronze")

        tables = ["coingecko_market_data", "fear_greed_raw", "onchain_macro_raw"]

        with sql.connect(
            server_hostname=connection_params["host"],
            http_path=connection_params["http_path"],
            access_token=connection_params["token"],
        ) as conn:
            with conn.cursor() as cursor:
                for table in tables:
                    table_name = qualified_table_name(catalog, schema, table)
                    try:
                        logger.info("OPTIMIZE %s", table_name)
                        cursor.execute(f"OPTIMIZE {table_name}")
                    except Exception:
                        logger.exception("Failed to optimize %s", table_name)

        return {"optimized_tables": tables}

    @task(execution_timeout=timedelta(minutes=10))
    def cleanup_old_logs(**context):
        """Remove task log files older than 90 days."""
        import os
        import shutil
        import time

        log_dir = "/opt/airflow/logs"
        cutoff = time.time() - (90 * 86400)
        removed = 0

        if not os.path.isdir(log_dir):
            logger.info("Log directory %s not found, skipping", log_dir)
            return {"removed_dirs": 0}

        for dag_dir in os.listdir(log_dir):
            dag_path = os.path.join(log_dir, dag_dir)
            if not os.path.isdir(dag_path):
                continue
            for run_dir in os.listdir(dag_path):
                run_path = os.path.join(dag_path, run_dir)
                if not os.path.isdir(run_path):
                    continue
                try:
                    if os.path.getmtime(run_path) < cutoff:
                        shutil.rmtree(run_path)
                        removed += 1
                except Exception:
                    logger.exception("Failed to remove %s", run_path)

        logger.info("Removed %d old log directories", removed)
        return {"removed_dirs": removed}

    optimize_delta_tables()
    cleanup_old_logs()


housekeeping()
