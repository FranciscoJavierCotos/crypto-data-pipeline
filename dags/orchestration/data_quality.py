# ---------------------------------------------------------------------------
# WHAT CHANGED (vs crypto_data_quality_pipeline.py):
#   1. Converted to TaskFlow @dag decorator.
#   2. dag_id renamed: quality_checks_crypto_data_layers → data_quality.
#   3. Added tags, doc_md.
#   4. Bash script logic preserved — passes layer, bronze_source, run_key,
#      quality_grace_days, fail_on_non_critical from dag_run.conf.
#   5. File moved to dags/orchestration/.
#
# NOTE: The bash script is intentionally lengthy — it selects critical vs
# non-critical dbt tests per layer and runs them separately.
# ---------------------------------------------------------------------------
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator

from callbacks import task_failure_alert

DOC_MD = """
### Data Quality Checks

Runs dbt tests across bronze, silver, and/or gold layers.
Triggered by ingestion and transformation DAGs with layer-specific config.

**Parameters (via `dag_run.conf`):**
- `layer`: bronze | silver | gold | all
- `bronze_source`: coingecko | fear_greed | onchain
- `run_key`: batch identifier for run-scoped bronze tests
- `quality_grace_days`: lookback window for freshness (default: 1)
- `fail_on_non_critical`: whether non-critical test failures fail the DAG
"""

default_args = {
    "owner": "data-engineering",
    "start_date": datetime(2025, 3, 21),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "email_on_failure": True,
    "on_failure_callback": task_failure_alert,
}


@dag(
    dag_id="data_quality",
    default_args=default_args,
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    tags=["quality", "crypto", "dbt", "testing"],
    doc_md=DOC_MD,
    params={
        "layer": "all",
        "bronze_source": "coingecko",
        "run_key": "",
        "quality_grace_days": 1,
        "fail_on_non_critical": False,
    },
)
def data_quality():

    run_dbt_quality_tests = BashOperator(
        task_id="run_dbt_quality_tests",
        retries=0,
        execution_timeout=timedelta(minutes=20),
        append_env=True,
        bash_command="""
            set -euo pipefail
            cd /opt/airflow/dbt
            export PATH="/home/airflow/.local/bin:${PATH}"

            DBT_BIN="${DBT_BIN:-/home/airflow/.local/bin/dbt}"
            if [ ! -x "$DBT_BIN" ]; then
                echo "dbt executable not found at $DBT_BIN"
                exit 1
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
                echo "Missing DATABRICKS_TOKEN."
                exit 1
            fi

            export DATABRICKS_AUTH_TYPE="${DATABRICKS_AUTH_TYPE:-pat}"

            LAYER='{{ (dag_run.conf.get("layer", params.layer) if dag_run and dag_run.conf else params.layer) | lower }}'
            BRONZE_SOURCE='{{ (dag_run.conf.get("bronze_source", params.bronze_source) if dag_run and dag_run.conf else params.bronze_source) | lower }}'
            RUN_KEY='{{ dag_run.conf.get("run_key", params.run_key) if dag_run and dag_run.conf else params.run_key }}'
            QUALITY_GRACE_DAYS='{{ dag_run.conf.get("quality_grace_days", params.quality_grace_days) if dag_run and dag_run.conf else params.quality_grace_days }}'
            FAIL_ON_NON_CRITICAL='{{ dag_run.conf.get("fail_on_non_critical", params.fail_on_non_critical) if dag_run and dag_run.conf else params.fail_on_non_critical }}'

            case "$LAYER" in
                bronze|silver|gold|all) ;;
                *) echo "Unsupported layer '$LAYER'"; exit 1 ;;
            esac

            case "$BRONZE_SOURCE" in
                coingecko|fear_greed|onchain) ;;
                *) echo "Unsupported bronze_source '$BRONZE_SOURCE'"; exit 1 ;;
            esac

            DBT_TARGET="${DBT_TARGET:-prod}"
            DBT_THREADS="${DBT_THREADS:-1}"
            DBT_TIMEOUT_SECONDS="${DBT_TEST_TIMEOUT_SECONDS:-600}"

            echo "Quality run: layer=$LAYER bronze_source=$BRONZE_SOURCE run_key=${RUN_KEY:-<empty>} grace=$QUALITY_GRACE_DAYS fail_non_critical=$FAIL_ON_NON_CRITICAL"

            print_dbt_failure_summary() {
                local run_results_file="target/run_results.json"
                if [ ! -f "$run_results_file" ]; then
                    echo "WARNING: dbt returned non-zero but $run_results_file was not found. See full dbt output above."
                    return 0
                fi

                python - <<'PY'
import json

path = "target/run_results.json"
try:
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
except Exception as exc:  # noqa: BLE001
    print(f"WARNING: Could not parse {path}: {exc}")
    raise SystemExit(0)

results = payload.get("results") or []
failures = [r for r in results if r.get("status") in {"fail", "error"}]
if not failures:
    print("WARNING: dbt failed but run_results.json had no fail/error records.")
    raise SystemExit(0)

print("WARNING: Data quality issues detected:")
for item in failures[:20]:
    node_id = item.get("unique_id", "unknown_test")
    message = (item.get("message") or "").strip().replace("\\n", " ")
    if message:
        print(f" - {node_id}: {message}")
    else:
        print(f" - {node_id}")

remaining = len(failures) - 20
if remaining > 0:
    print(f" - ... and {remaining} more failures")
PY
            }

            run_dbt_tests() {
                TEST_CLASS="$1"; shift
                if [ "$#" -eq 0 ]; then
                    echo "No selectors for $TEST_CLASS tests"; return 0
                fi
                DBT_CMD=("$DBT_BIN" test --target "$DBT_TARGET" --threads "$DBT_THREADS" --select "$@")
                DBT_VARS="{quality_grace_days: $QUALITY_GRACE_DAYS"
                if [ -n "$RUN_KEY" ]; then
                    RUN_KEY_ESCAPED="${RUN_KEY//\\'/\\'\\'\\'}"
                    DBT_VARS="{quality_grace_days: $QUALITY_GRACE_DAYS, run_key: '$RUN_KEY_ESCAPED'}"
                else
                    DBT_VARS="$DBT_VARS}"
                fi
                DBT_CMD+=(--vars "$DBT_VARS")
                echo "Running $TEST_CLASS tests: $*"

                DBT_EXIT=0
                if command -v timeout >/dev/null 2>&1; then
                    if timeout --signal=TERM "$DBT_TIMEOUT_SECONDS" "${DBT_CMD[@]}"; then
                        DBT_EXIT=0
                    else
                        DBT_EXIT=$?
                    fi
                else
                    if "${DBT_CMD[@]}"; then
                        DBT_EXIT=0
                    else
                        DBT_EXIT=$?
                    fi
                fi

                if [ "$DBT_EXIT" -ne 0 ]; then
                    echo "WARNING: $TEST_CLASS dbt tests failed with exit code $DBT_EXIT"
                    print_dbt_failure_summary
                fi

                return "$DBT_EXIT"
            }

            CRITICAL_SELECTORS=()
            NON_CRITICAL_SELECTORS=()
            SKIP_COINGECKO_BRONZE=false

            if [ "$LAYER" = "bronze" ] || [ "$LAYER" = "all" ]; then
                if [ "$BRONZE_SOURCE" = "coingecko" ]; then
                    if [ -z "$RUN_KEY" ]; then
                        if [ "$LAYER" = "all" ]; then
                            echo "run_key missing for coingecko bronze while layer=all. Skipping."
                            SKIP_COINGECKO_BRONZE=true
                        else
                            echo "run_key required for bronze_source=coingecko"; exit 1
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
                        NON_CRITICAL_SELECTORS+=(path:models/Sources path:models/sources path:models/bronze)
                    fi
                fi
                if [ "$BRONZE_SOURCE" = "fear_greed" ]; then
                    CRITICAL_SELECTORS+=(
                        path:tests/bronze/bronze_fear_greed_no_duplicate_metric_date.sql
                        path:tests/bronze/bronze_fear_greed_value_range.sql
                    )
                    NON_CRITICAL_SELECTORS+=(path:models/Sources path:models/sources path:models/bronze)
                fi
                if [ "$BRONZE_SOURCE" = "onchain" ]; then
                    CRITICAL_SELECTORS+=(
                        path:tests/bronze/bronze_onchain_no_duplicate_metric_date.sql
                        path:tests/bronze/bronze_onchain_metrics_non_negative.sql
                    )
                    NON_CRITICAL_SELECTORS+=(path:models/Sources path:models/sources path:models/bronze)
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
                NON_CRITICAL_SELECTORS+=(path:tests/gold/gold_market_summary_min_coverage.sql)
                NON_CRITICAL_SELECTORS+=(path:tests/gold/gold_sentiment_vs_price_min_coverage.sql)
                NON_CRITICAL_SELECTORS+=(path:tests/gold/gold_volatility_signal_min_coverage.sql)
                NON_CRITICAL_SELECTORS+=(path:tests/gold/gold_liquidity_ranking_min_coverage.sql)
                NON_CRITICAL_SELECTORS+=(path:tests/gold/gold_btc_dominance_min_coverage.sql)
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
                    NC_EXIT=$?
                    echo "Non-critical checks failed (exit $NC_EXIT)."
                    if [ "${FAIL_ON_NON_CRITICAL,,}" = "true" ]; then
                        exit "$NC_EXIT"
                    fi
                    echo "Continuing (fail_on_non_critical=false)"
                fi
            else
                echo "No non-critical tests for layer=$LAYER"
            fi
        """,
        env={
            "DBT_PROFILES_DIR": "/opt/airflow/dbt",
            "DBT_BIN": "/home/airflow/.local/bin/dbt",
            "DATABRICKS_AUTH_TYPE": "pat",
        },
    )


data_quality()
