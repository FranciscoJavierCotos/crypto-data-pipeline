from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 21),
    "retries": 0,
}

with DAG(
    dag_id="coingecko_quality_pipeline",
    default_args=default_args,
    schedule=None,
    catchup=False,
    params={
        "layer": "all",
        "batch_id": "",
        "fail_on_non_critical": False,
    },
) as dag:

    run_dbt_quality_tests = BashOperator(
        task_id="run_dbt_quality_tests",
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

            LAYER='{{ (dag_run.conf.get("layer", params.layer) if dag_run and dag_run.conf else params.layer) | lower }}'
            BATCH_ID='{{ dag_run.conf.get("batch_id", params.batch_id) if dag_run and dag_run.conf else params.batch_id }}'
            FAIL_ON_NON_CRITICAL='{{ dag_run.conf.get("fail_on_non_critical", params.fail_on_non_critical) if dag_run and dag_run.conf else params.fail_on_non_critical }}'

            case "$LAYER" in
                bronze|silver|gold|all)
                    ;;
                *)
                    echo "Unsupported layer '$LAYER'. Expected one of: bronze, silver, gold, all"
                    exit 1
                    ;;
            esac

            DBT_TARGET="${DBT_TARGET:-prod}"
            DBT_THREADS="${DBT_THREADS:-1}"
            DBT_TIMEOUT_SECONDS="${DBT_TEST_TIMEOUT_SECONDS:-600}"

            run_dbt_tests() {
                TEST_CLASS="$1"
                shift

                if [ "$#" -eq 0 ]; then
                    echo "No selectors configured for $TEST_CLASS tests"
                    return 0
                fi

                DBT_CMD=(
                    "$DBT_BIN" test
                    --target "$DBT_TARGET"
                    --threads "$DBT_THREADS"
                    --select "$@"
                )

                # Bronze batch tests require a batch_id; only pass vars when available.
                if [ -n "$BATCH_ID" ]; then
                    DBT_CMD+=(--vars "{batch_id: '$BATCH_ID'}")
                fi

                echo "Running $TEST_CLASS dbt tests for layer=$LAYER"
                if command -v timeout >/dev/null 2>&1; then
                    set +e
                    timeout --signal=TERM "$DBT_TIMEOUT_SECONDS" "${DBT_CMD[@]}"
                    DBT_EXIT_CODE=$?
                    set -e
                    if [ "$DBT_EXIT_CODE" -eq 124 ]; then
                        echo "$TEST_CLASS dbt tests timed out after ${DBT_TIMEOUT_SECONDS}s"
                    fi
                    return "$DBT_EXIT_CODE"
                fi

                set +e
                "${DBT_CMD[@]}"
                DBT_EXIT_CODE=$?
                set -e
                return "$DBT_EXIT_CODE"
            }

            CRITICAL_SELECTORS=()
            NON_CRITICAL_SELECTORS=()

            if [ "$LAYER" = "bronze" ] || [ "$LAYER" = "all" ]; then
                if [ -z "$BATCH_ID" ]; then
                    echo "batch_id is required for bronze quality checks. Provide it in DAG run config."
                    exit 1
                fi

                CRITICAL_SELECTORS+=(
                    path:tests/bronze/bronze_batch_has_data.sql
                    path:tests/bronze/bronze_batch_no_duplicate_coin_ids.sql
                    path:tests/bronze/bronze_batch_no_empty_required_fields.sql
                )
                NON_CRITICAL_SELECTORS+=(path:tests/bronze/bronze_batch_numeric_values_sane.sql)
                NON_CRITICAL_SELECTORS+=(path:models/Sources)
            fi

            if [ "$LAYER" = "silver" ] || [ "$LAYER" = "all" ]; then
                NON_CRITICAL_SELECTORS+=(path:models/silver)
            fi

            if [ "$LAYER" = "gold" ] || [ "$LAYER" = "all" ]; then
                NON_CRITICAL_SELECTORS+=(path:models/gold)
            fi

            if [ "${#CRITICAL_SELECTORS[@]}" -gt 0 ]; then
                run_dbt_tests "critical" "${CRITICAL_SELECTORS[@]}"
            else
                echo "No critical tests selected for layer=$LAYER"
            fi

            if [ "${#NON_CRITICAL_SELECTORS[@]}" -gt 0 ]; then
                set +e
                run_dbt_tests "non-critical" "${NON_CRITICAL_SELECTORS[@]}"
                NON_CRITICAL_EXIT_CODE=$?
                set -e

                if [ "$NON_CRITICAL_EXIT_CODE" -ne 0 ]; then
                    echo "Non-critical quality checks failed (exit $NON_CRITICAL_EXIT_CODE)."
                    if [ "${FAIL_ON_NON_CRITICAL,,}" = "true" ]; then
                        exit "$NON_CRITICAL_EXIT_CODE"
                    fi
                    echo "Continuing by policy because fail_on_non_critical=false"
                fi
            else
                echo "No non-critical tests selected for layer=$LAYER"
            fi
        """,
        env={
            "DBT_PROFILES_DIR": "/opt/airflow/dbt",
            "DBT_BIN": "/home/airflow/.local/bin/dbt",
            "DATABRICKS_AUTH_TYPE": "pat",
        },
    )
