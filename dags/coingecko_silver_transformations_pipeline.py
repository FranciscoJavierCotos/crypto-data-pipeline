from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 21),
    "retries": 1,
}

with DAG(
    dag_id="coingecko_silver_transformations_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
) as dag:

    run_dbt_silver_transformations = BashOperator(
        task_id="run_dbt_silver_transformations",
        execution_timeout=timedelta(minutes=20),
        append_env=True,
        bash_command="""
            set -euo pipefail
            cd /opt/airflow/dbt
            export PATH="/home/airflow/.local/bin:${PATH}"

            DBT_BIN="${DBT_BIN:-/home/airflow/.local/bin/dbt}"
            if [ ! -x "$DBT_BIN" ]; then
                echo "dbt executable not found at $DBT_BIN"
                echo "PATH=$PATH"
                exit 1
            fi

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
                echo "Missing DATABRICKS_TOKEN. Set it in .env or include token in AIRFLOW_CONN_DATABRICKS_DEFAULT."
                exit 1
            fi

            export DATABRICKS_AUTH_TYPE="${DATABRICKS_AUTH_TYPE:-pat}"
            DBT_TIMEOUT_SECONDS="${DBT_RUN_TIMEOUT_SECONDS:-900}"
            DBT_THREADS="${DBT_THREADS:-1}"

            DBT_CMD=(
                "$DBT_BIN" run
                --target "${DBT_TARGET:-prod}"
                --threads "$DBT_THREADS"
                --select path:models/silver
            )

            if command -v timeout >/dev/null 2>&1; then
                timeout --signal=TERM "$DBT_TIMEOUT_SECONDS" "${DBT_CMD[@]}"
                exit $?
            fi

            "${DBT_CMD[@]}"
        """,
        env={
            "DBT_PROFILES_DIR": "/opt/airflow/dbt",
            "DBT_BIN": "/home/airflow/.local/bin/dbt",
            "DATABRICKS_AUTH_TYPE": "pat",
        },
    )

    trigger_quality_after_silver = TriggerDagRunOperator(
        task_id="trigger_quality_after_silver",
        trigger_dag_id="coingecko_quality_pipeline",
        conf={
            "layer": "silver",
            "fail_on_non_critical": True,
        },
    )

    trigger_gold_transformations = TriggerDagRunOperator(
        task_id="trigger_gold_transformations",
        trigger_dag_id="coingecko_gold_transformations_pipeline",
    )

    run_dbt_silver_transformations >> trigger_quality_after_silver >> trigger_gold_transformations
