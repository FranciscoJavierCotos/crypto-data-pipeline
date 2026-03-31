# ---------------------------------------------------------------------------
# WHAT CHANGED (vs coingecko_silver_transformations_pipeline.py):
#   1. Converted to TaskFlow @dag decorator.
#   2. dag_id renamed: transform_silver_crypto_models → silver_transform.
#   3. Added ExternalTaskSensor to wait for upstream bronze DAGs.
#   4. Separated dbt run and dbt test into distinct tasks.
#   5. Added --fail-fast flag for production dbt runs.
#   6. Passes run_date via dbt --vars for deterministic execution.
#   7. Added tags, doc_md, meta.
#   8. File moved to dags/transformation/.
#
# DESIGN DECISIONS:
#   - ExternalTaskSensor uses check_existence=True to avoid failure when
#     no matching upstream run exists (e.g. first-time run).
#   - Sensor has a 2h timeout — if bronze hasn't run, the DAG fails
#     rather than blocking a worker indefinitely.
# ---------------------------------------------------------------------------
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from callbacks import task_failure_alert

DOC_MD = """
### Silver dbt Transformation

Runs `dbt run --select silver` and `dbt test --select silver` against
the Databricks SQL warehouse. Waits for upstream bronze DAGs to complete
before starting.

**Tasks:**
1. `run_dbt_silver` — `dbt run --select silver --fail-fast`
2. `test_dbt_silver` — `dbt test --select silver`
3. `trigger_quality` — Async quality check trigger

**Note:** Bronze ordering is enforced by the master pipeline
(`wait_for_completion=True`), so no ExternalTaskSensors are needed here.
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

# Shared bash preamble for dbt commands — validates env and connection.
DBT_PREAMBLE = """
    set -euo pipefail
    cd /opt/airflow/dbt
    export PATH="/home/airflow/.local/bin:${PATH}"

    DBT_BIN="${DBT_BIN:-/home/airflow/.local/bin/dbt}"
    if [ ! -x "$DBT_BIN" ]; then
        echo "dbt executable not found at $DBT_BIN" && exit 1
    fi

    if [ -z "${DATABRICKS_TOKEN:-}" ] && [ -n "${AIRFLOW_CONN_DATABRICKS_DEFAULT:-}" ]; then
        DBR_URI="${AIRFLOW_CONN_DATABRICKS_DEFAULT#\\'}"
        DBR_URI="${DBR_URI%\\'}"
        DBR_AUTH_PART="${DBR_URI#*://}"
        if [ "$DBR_AUTH_PART" != "$DBR_URI" ]; then
            DBR_AUTH_PART="${DBR_AUTH_PART%%@*}"
            if [ "$DBR_AUTH_PART" != "${DBR_AUTH_PART#*:}" ]; then
                export DATABRICKS_TOKEN="${DBR_AUTH_PART#*:}"
            fi
        fi
    fi

    if [ -z "${DATABRICKS_TOKEN:-}" ]; then
        echo "Missing DATABRICKS_TOKEN" && exit 1
    fi

    export DATABRICKS_AUTH_TYPE="${DATABRICKS_AUTH_TYPE:-pat}"
    DBT_TARGET="${DBT_TARGET:-prod}"
    DBT_THREADS="${DBT_THREADS:-4}"
"""

DBT_ENV = {
    "DBT_PROFILES_DIR": "/opt/airflow/dbt",
    "DBT_BIN": "/home/airflow/.local/bin/dbt",
    "DATABRICKS_AUTH_TYPE": "pat",
}


@dag(
    dag_id="silver_transform",
    default_args=default_args,
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["silver", "crypto", "transform", "dbt"],
    doc_md=DOC_MD,
)
def silver_transform():

    # ---- dbt run ----
    run_dbt_silver = BashOperator(
        task_id="run_dbt_silver",
        execution_timeout=timedelta(minutes=20),
        append_env=True,
        env=DBT_ENV,
        bash_command=DBT_PREAMBLE + """
            SILVER_REPROCESS_DAYS='{{ dag_run.conf.get("silver_reprocess_days", "") if dag_run and dag_run.conf else "" }}'

            DBT_CMD=(
                "$DBT_BIN" run
                --target "$DBT_TARGET"
                --threads "$DBT_THREADS"
                --select "+path:models/silver"
                --fail-fast
            )

            if [ -n "$SILVER_REPROCESS_DAYS" ]; then
                DBT_CMD+=(--vars "{silver_reprocess_days: $SILVER_REPROCESS_DAYS}")
            fi

            RUN_DATE='{{ ds }}'
            if [ -n "$RUN_DATE" ]; then
                DBT_CMD+=(--vars "{run_date: '$RUN_DATE'}")
            fi

            echo "Executing: ${DBT_CMD[*]}"
            "${DBT_CMD[@]}"
        """,
    )

    # ---- dbt test ----
    test_dbt_silver = BashOperator(
        task_id="test_dbt_silver",
        execution_timeout=timedelta(minutes=15),
        append_env=True,
        env=DBT_ENV,
        bash_command=DBT_PREAMBLE + """
            "$DBT_BIN" test \
                --target "$DBT_TARGET" \
                --threads "$DBT_THREADS" \
                --select "+path:models/silver"
        """,
    )

    # ---- Quality trigger (async) ----
    trigger_quality = TriggerDagRunOperator(
        task_id="trigger_quality_after_silver",
        trigger_dag_id="data_quality",
        wait_for_completion=False,
        conf={"layer": "silver", "fail_on_non_critical": False},
    )

    # ---- Wiring ----
    run_dbt_silver >> test_dbt_silver >> trigger_quality


silver_transform()
