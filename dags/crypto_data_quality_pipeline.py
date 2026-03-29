from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
from pipeline_callbacks import task_failure_alert


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 21),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": task_failure_alert,
}

with DAG(
    dag_id="quality_checks_crypto_data_layers",
    default_args=default_args,
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    params={
        "layer": "all",
        "bronze_source": "coingecko",
        "run_key": "",
        "quality_grace_days": 1,
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
            BRONZE_SOURCE='{{ (dag_run.conf.get("bronze_source", params.bronze_source) if dag_run and dag_run.conf else params.bronze_source) | lower }}'
            RUN_KEY='{{ dag_run.conf.get("run_key", params.run_key) if dag_run and dag_run.conf else params.run_key }}'
            QUALITY_GRACE_DAYS='{{ dag_run.conf.get("quality_grace_days", params.quality_grace_days) if dag_run and dag_run.conf else params.quality_grace_days }}'
            FAIL_ON_NON_CRITICAL='{{ dag_run.conf.get("fail_on_non_critical", params.fail_on_non_critical) if dag_run and dag_run.conf else params.fail_on_non_critical }}'

            case "$LAYER" in
                bronze|silver|gold|all)
                    ;;
                *)
                    echo "Unsupported layer '$LAYER'. Expected one of: bronze, silver, gold, all"
                    exit 1
                    ;;
            esac

            case "$BRONZE_SOURCE" in
                coingecko|fear_greed|onchain)
                    ;;
                *)
                    echo "Unsupported bronze_source '$BRONZE_SOURCE'. Expected one of: coingecko, fear_greed, onchain"
                    exit 1
                    ;;
            esac

            case "$QUALITY_GRACE_DAYS" in
                ''|*[!0-9]*)
                    echo "Unsupported quality_grace_days '$QUALITY_GRACE_DAYS'. Expected a non-negative integer"
                    exit 1
                    ;;
                *)
                    ;;
            esac

            DBT_TARGET="${DBT_TARGET:-prod}"
            DBT_THREADS="${DBT_THREADS:-1}"
            DBT_TIMEOUT_SECONDS="${DBT_TEST_TIMEOUT_SECONDS:-600}"

            echo "Quality run context: layer=$LAYER bronze_source=$BRONZE_SOURCE run_key=${RUN_KEY:-<empty>} quality_grace_days=$QUALITY_GRACE_DAYS fail_on_non_critical=$FAIL_ON_NON_CRITICAL"

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

                DBT_VARS="{quality_grace_days: $QUALITY_GRACE_DAYS"
                if [ -n "$RUN_KEY" ]; then
                    # Escape single quotes for YAML single-quoted scalar values.
                    RUN_KEY_ESCAPED="${RUN_KEY//\'/'\''}"
                    DBT_VARS=", run_key: '$RUN_KEY_ESCAPED'}"
                    DBT_VARS="{quality_grace_days: $QUALITY_GRACE_DAYS${DBT_VARS}"
                else
                    DBT_VARS="$DBT_VARS}"
                fi
                DBT_CMD+=(--vars "$DBT_VARS")

                echo "Running $TEST_CLASS dbt tests for layer=$LAYER"
                echo "Selectors: $*"
                if command -v timeout >/dev/null 2>&1; then
                    if timeout --signal=TERM "$DBT_TIMEOUT_SECONDS" "${DBT_CMD[@]}"; then
                        return 0
                    else
                        DBT_EXIT_CODE=$?
                        if [ "$DBT_EXIT_CODE" -eq 124 ]; then
                            echo "$TEST_CLASS dbt tests timed out after ${DBT_TIMEOUT_SECONDS}s"
                        fi
                        return "$DBT_EXIT_CODE"
                    fi
                fi

                if "${DBT_CMD[@]}"; then
                    return 0
                else
                    DBT_EXIT_CODE=$?
                    return "$DBT_EXIT_CODE"
                fi
            }

            CRITICAL_SELECTORS=()
            NON_CRITICAL_SELECTORS=()
            SKIP_COINGECKO_BRONZE=false

            if [ "$LAYER" = "bronze" ] || [ "$LAYER" = "all" ]; then
                if [ "$BRONZE_SOURCE" = "coingecko" ]; then
                    if [ -z "$RUN_KEY" ]; then
                        if [ "$LAYER" = "all" ]; then
                            echo "run_key missing for bronze_source=coingecko while layer=all. Skipping run-scoped coingecko bronze checks and continuing with other selected layers."
                            SKIP_COINGECKO_BRONZE=true
                        else
                            echo "run_key is required for bronze_source=coingecko quality checks. Provide it in DAG run config."
                            exit 1
                        fi
                    fi

                    if [ "$SKIP_COINGECKO_BRONZE" = "false" ]; then
                        CRITICAL_SELECTORS+=(
                            path:tests/bronze/bronze_batch_has_data.sql
                            path:tests/bronze/bronze_batch_no_duplicate_coin_ids.sql
                            path:tests/bronze/bronze_batch_no_empty_required_fields.sql
                            path:tests/bronze/bronze_batch_price_bounds_consistent.sql
                        )
                        NON_CRITICAL_SELECTORS+=(path:tests/bronze/bronze_batch_numeric_values_sane.sql)
                        NON_CRITICAL_SELECTORS+=(path:tests/bronze/bronze_batch_no_future_timestamps.sql)
                        NON_CRITICAL_SELECTORS+=(path:models/Sources)
                        NON_CRITICAL_SELECTORS+=(path:models/bronze)
                    fi
                fi

                if [ "$BRONZE_SOURCE" = "fear_greed" ]; then
                    CRITICAL_SELECTORS+=(
                        path:tests/bronze/bronze_fear_greed_no_duplicate_metric_date.sql
                        path:tests/bronze/bronze_fear_greed_value_range.sql
                    )
                    NON_CRITICAL_SELECTORS+=(path:models/Sources)
                    NON_CRITICAL_SELECTORS+=(path:models/bronze)
                fi

                if [ "$BRONZE_SOURCE" = "onchain" ]; then
                    CRITICAL_SELECTORS+=(
                        path:tests/bronze/bronze_onchain_no_duplicate_metric_date.sql
                        path:tests/bronze/bronze_onchain_metrics_non_negative.sql
                    )
                    NON_CRITICAL_SELECTORS+=(path:models/Sources)
                    NON_CRITICAL_SELECTORS+=(path:models/bronze)
                fi
            fi

            if [ "$LAYER" = "silver" ] || [ "$LAYER" = "all" ]; then
                CRITICAL_SELECTORS+=(
                    path:tests/silver/silver_no_duplicate_coin_date.sql
                    path:tests/silver/silver_positive_prices.sql
                    path:tests/silver/silver_high_gte_low.sql
                    path:tests/silver/silver_required_identifiers.sql
                    path:tests/silver/silver_market_cap_rank_valid.sql
                    path:tests/silver/silver_fear_greed_label_values.sql
                )
                NON_CRITICAL_SELECTORS+=(path:models/silver)
                NON_CRITICAL_SELECTORS+=(path:tests/silver/silver_fear_greed_range.sql)
                NON_CRITICAL_SELECTORS+=(path:tests/silver/silver_volume_to_market_cap_ratio_sane.sql)
            fi

            if [ "$LAYER" = "gold" ] || [ "$LAYER" = "all" ]; then
                CRITICAL_SELECTORS+=(
                    path:tests/gold/gold_btc_dominance_range.sql
                    path:tests/gold/gold_dominance_sums_to_100.sql
                    path:tests/gold/gold_market_summary_no_duplicates.sql
                )
                NON_CRITICAL_SELECTORS+=(path:models/gold)
                NON_CRITICAL_SELECTORS+=(path:tests/gold/gold_volatility_bucket_values.sql)
                NON_CRITICAL_SELECTORS+=(path:tests/gold/gold_btc_price_in_btc_is_one.sql)
                NON_CRITICAL_SELECTORS+=(path:tests/gold/gold_market_summary_data_completeness_range.sql)
                NON_CRITICAL_SELECTORS+=(path:tests/gold/gold_unknown_market_cap_assets_non_negative.sql)
            fi

            if [ "${CRITICAL_SELECTORS[@]+x}" = "x" ]; then
                run_dbt_tests "critical" "${CRITICAL_SELECTORS[@]}"
            else
                echo "No critical tests selected for layer=$LAYER"
            fi

            if [ "${NON_CRITICAL_SELECTORS[@]+x}" = "x" ]; then
                if run_dbt_tests "non-critical" "${NON_CRITICAL_SELECTORS[@]}"; then
                    :
                else
                    NON_CRITICAL_EXIT_CODE=$?
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
