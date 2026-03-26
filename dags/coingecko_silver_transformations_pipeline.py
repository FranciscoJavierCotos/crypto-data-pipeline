from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from pipeline_callbacks import task_failure_alert


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 21),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": task_failure_alert,
}

with DAG(
    dag_id="transform_silver_crypto_models",
    default_args=default_args,
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
) as dag:

    run_dbt_silver_transformations = BashOperator(
        task_id="run_dbt_silver_transformations",
        execution_timeout=timedelta(minutes=20),
        append_env=True,
        bash_command="""
            set -euo pipefail

            log() {
                echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] [silver_dag] $*"
            }

            STEP_START_TS="$(date +%s)"

            log "Starting dbt silver transformations"
            log "AIRFLOW_DAG_ID=${AIRFLOW_CTX_DAG_ID:-unknown} AIRFLOW_TASK_ID=${AIRFLOW_CTX_TASK_ID:-unknown} RUN_ID=${AIRFLOW_CTX_DAG_RUN_ID:-unknown}"
            cd /opt/airflow/dbt
            log "Working directory: $(pwd)"
            export PATH="/home/airflow/.local/bin:${PATH}"

            DBT_BIN="${DBT_BIN:-/home/airflow/.local/bin/dbt}"
            if [ ! -x "$DBT_BIN" ]; then
                log "dbt executable not found at $DBT_BIN"
                log "PATH=$PATH"
                exit 1
            fi

            log "dbt executable: $DBT_BIN"

            if [ -z "${DATABRICKS_TOKEN:-}" ] && [ -n "${AIRFLOW_CONN_DATABRICKS_DEFAULT:-}" ]; then
                DBR_URI="${AIRFLOW_CONN_DATABRICKS_DEFAULT#\'}"
                DBR_URI="${DBR_URI%\'}"
                DBR_AUTH_PART="${DBR_URI#*://}"
                if [ "$DBR_AUTH_PART" != "$DBR_URI" ]; then
                    DBR_AUTH_PART="${DBR_AUTH_PART%%@*}"
                    if [ "$DBR_AUTH_PART" != "${DBR_AUTH_PART#*:}" ]; then
                        export DATABRICKS_TOKEN="${DBR_AUTH_PART#*:}"
                    fi
                fi
            fi

            if [ -z "${DATABRICKS_TOKEN:-}" ]; then
                log "Missing DATABRICKS_TOKEN. Set it in .env or include token in AIRFLOW_CONN_DATABRICKS_DEFAULT."
                exit 1
            fi

            export DATABRICKS_AUTH_TYPE="${DATABRICKS_AUTH_TYPE:-pat}"
            DBT_TIMEOUT_SECONDS="${DBT_RUN_TIMEOUT_SECONDS:-900}"
            DBT_THREADS="${DBT_THREADS:-4}"
            DBT_LOG_LEVEL="${DBT_LOG_LEVEL:-info}"

            log "Configuration: target=${DBT_TARGET:-prod} threads=${DBT_THREADS} timeout=${DBT_TIMEOUT_SECONDS}s log_level=${DBT_LOG_LEVEL}"

            log "Running dbt debug to validate connection"
            "$DBT_BIN" debug --target "${DBT_TARGET:-prod}" --log-level "$DBT_LOG_LEVEL"

            DBT_SELECTOR="${DBT_SELECTOR:-+path:models/silver}"

            log "Discovering selected models with selector: ${DBT_SELECTOR}"
            "$DBT_BIN" ls --target "${DBT_TARGET:-prod}" --select "$DBT_SELECTOR" --resource-type model --output name --log-level "$DBT_LOG_LEVEL"

            DBT_CMD=(
                "$DBT_BIN" run
                --target "${DBT_TARGET:-prod}"
                --threads "$DBT_THREADS"
                --log-level "$DBT_LOG_LEVEL"
                --select "$DBT_SELECTOR"
            )

            log "Executing: ${DBT_CMD[*]}"

            if command -v timeout >/dev/null 2>&1; then
                timeout --signal=TERM "$DBT_TIMEOUT_SECONDS" "${DBT_CMD[@]}"
                EXIT_CODE=$?
                DURATION=$(( $(date +%s) - STEP_START_TS ))
                log "dbt run finished with exit code ${EXIT_CODE} after ${DURATION}s"
                exit $EXIT_CODE
            fi

            "${DBT_CMD[@]}"
            DURATION=$(( $(date +%s) - STEP_START_TS ))
            log "dbt run finished successfully after ${DURATION}s"
        """,
        env={
            "DBT_PROFILES_DIR": "/opt/airflow/dbt",
            "DBT_BIN": "/home/airflow/.local/bin/dbt",
            "DATABRICKS_AUTH_TYPE": "pat",
        },
    )

    trigger_quality_after_silver = TriggerDagRunOperator(
        task_id="trigger_quality_after_silver",
        trigger_dag_id="quality_checks_crypto_data_layers",
        # Block until quality checks complete so downstream orchestration cannot skip failed quality.
        wait_for_completion=True,
        conf={
            "layer": "silver",
            "fail_on_non_critical": True,
        },
    )

    run_dbt_silver_transformations >> trigger_quality_after_silver
