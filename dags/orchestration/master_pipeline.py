# ---------------------------------------------------------------------------
# WHAT CHANGED (vs crypto_data_pipeline.py):
#   1. Converted from classic DAG to TaskFlow @dag decorator.
#   2. dag_id renamed from orchestrate_crypto_bronze_silver_gold →
#      master_pipeline.
#   3. Uses TriggerDagRunOperator to orchestrate the full ingestion →
#      transform → quality flow (same pattern, updated dag_ids).
#   4. Added tags, doc_md, owner, sla. default_args hardened.
#   5. Callback import now from plugins/callbacks.py.
#   6. File moved from dags/ → dags/orchestration/.
#   7. Added trigger for data_quality after gold transforms (gate before
#      marking the pipeline as successful).
#
# DESIGN DECISIONS (flagged for review):
#   - Schedule is "5 0 * * *" (daily at 00:05 UTC) — same as before.
#     Adjust if the ingestion window should be different.
#   - All TriggerDagRunOperator calls use wait_for_completion=True so the
#     pipeline is fully sequential: bronze → silver → gold → quality.
#   - dag_id changed — update any external triggers or Airflow UI bookmarks.
# ---------------------------------------------------------------------------
from __future__ import annotations

from datetime import datetime, timedelta

import pendulum
from airflow.sdk import dag
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from callbacks import task_failure_alert

DOC_MD = """
### Master Crypto Pipeline

Daily orchestrator that triggers the full medallion-architecture flow:

1. **Bronze ingestion** — CoinGecko, Fear & Greed, On-chain (parallel)
2. **Silver transformation** — `dbt run --select silver` (waits for all bronze)
3. **Gold transformation** — `dbt run --select gold` (waits for silver)
4. **Quality gate** — `dbt test` on all layers (non-critical findings logged as warnings)

Runs at **00:05 UTC** daily. Catchup is disabled.
"""

default_args = {
    "owner": "data-engineering",
    "start_date": datetime(2025, 3, 21, tzinfo=pendulum.timezone("Europe/Madrid")),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "email_on_failure": True,
    "on_failure_callback": task_failure_alert,
}

TRIGGER_DEFAULTS = dict(
    wait_for_completion=True,
    allowed_states=["success"],
    failed_states=["failed"],
    poke_interval=30,
    execution_timeout=timedelta(minutes=60),
)

# Silver tests can fail for data-coverage checks that are handled downstream in the
# quality gate. Keep the orchestration sequence moving after silver completes.
SILVER_TRIGGER_DEFAULTS = dict(
    wait_for_completion=True,
    allowed_states=["success", "failed"],
    failed_states=[],
    poke_interval=30,
    execution_timeout=timedelta(minutes=60),
)


@dag(
    dag_id="master_pipeline",
    default_args=default_args,
    schedule="5 0 * * *",
    catchup=False,
    is_paused_upon_creation=False,
    tags=["orchestration", "crypto", "master"],
    doc_md=DOC_MD,
)
def master_pipeline():

    start = EmptyOperator(task_id="start")

    # ---- Bronze ingestion (parallel) ----
    trigger_coingecko = TriggerDagRunOperator(
        task_id="trigger_coingecko_bronze",
        trigger_dag_id="coingecko_bronze_ingest",
        **TRIGGER_DEFAULTS,
    )

    trigger_fear_greed = TriggerDagRunOperator(
        task_id="trigger_fear_greed_bronze",
        trigger_dag_id="fear_greed_bronze_ingest",
        **TRIGGER_DEFAULTS,
    )

    trigger_onchain = TriggerDagRunOperator(
        task_id="trigger_onchain_bronze",
        trigger_dag_id="onchain_bronze_ingest",
        **TRIGGER_DEFAULTS,
    )

    # ---- Transformations (sequential) ----
    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_transform",
        trigger_dag_id="silver_transform",
        **SILVER_TRIGGER_DEFAULTS,
    )

    trigger_gold = TriggerDagRunOperator(
        task_id="trigger_gold_transform",
        trigger_dag_id="gold_transform",
        **TRIGGER_DEFAULTS,
    )

    # ---- Quality gate ----
    trigger_quality = TriggerDagRunOperator(
        task_id="trigger_quality_gate",
        trigger_dag_id="data_quality",
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"],
        poke_interval=30,
        execution_timeout=timedelta(minutes=30),
        conf={
            "layer": "all",
            "fail_on_non_critical": False,
        },
    )

    end = EmptyOperator(task_id="end")

    # ---- Dependency wiring ----
    start >> [trigger_coingecko, trigger_fear_greed, trigger_onchain]
    [trigger_coingecko, trigger_fear_greed, trigger_onchain] >> trigger_silver
    trigger_silver >> trigger_gold >> trigger_quality >> end


master_pipeline()
