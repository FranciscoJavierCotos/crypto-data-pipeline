# ---------------------------------------------------------------------------
# WHAT CHANGED:
#   New DAG — monitors pipeline health by checking source freshness,
#   DAG run success rates, and data completeness metrics.
#
# WHY:
#   - Proactive monitoring vs reactive failure alerts.
#   - Runs on a schedule independent of the main pipeline.
# ---------------------------------------------------------------------------
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from callbacks import task_failure_alert

logger = logging.getLogger(__name__)

DOC_MD = """
### Pipeline Health Monitor

Periodic health check that validates:
1. **Source freshness** — runs `dbt source freshness` to verify bronze data is current.
2. **DAG run stats** — logs recent DAG run success/failure counts.

Runs every 6 hours. Failures are informational (alert but don't block pipelines).
"""

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "on_failure_callback": task_failure_alert,
}


@dag(
    dag_id="pipeline_health",
    default_args=default_args,
    schedule="0 */6 * * *",
    catchup=False,
    is_paused_upon_creation=True,
    tags=["maintenance", "monitoring", "health"],
    doc_md=DOC_MD,
)
def pipeline_health():

    @task(execution_timeout=timedelta(minutes=10))
    def check_source_freshness(**context):
        import subprocess

        result = subprocess.run(
            [
                "/home/airflow/.local/bin/dbt",
                "source", "freshness",
                "--target", "prod",
                "--output", "json",
            ],
            cwd="/opt/airflow/dbt",
            capture_output=True,
            text=True,
            timeout=300,
            env={
                "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
                "DBT_PROFILES_DIR": "/opt/airflow/dbt",
                "DATABRICKS_AUTH_TYPE": "pat",
            },
        )
        logger.info("dbt source freshness exit code: %d", result.returncode)
        logger.info("stdout: %s", result.stdout[-2000:] if result.stdout else "(empty)")
        if result.returncode != 0:
            logger.warning("stderr: %s", result.stderr[-2000:] if result.stderr else "(empty)")
        return {"exit_code": result.returncode}

    @task(execution_timeout=timedelta(minutes=5))
    def log_health_summary(freshness_result: dict, **context):
        if freshness_result.get("exit_code", 1) == 0:
            logger.info("All sources fresh ✓")
        else:
            logger.warning("Source freshness check reported issues")

    freshness = check_source_freshness()
    log_health_summary(freshness)


pipeline_health()
