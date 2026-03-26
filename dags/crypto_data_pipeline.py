from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from pipeline_callbacks import task_failure_alert


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 21, tzinfo=pendulum.timezone("Europe/Madrid")),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": task_failure_alert,
}


with DAG(
    dag_id="orchestrate_crypto_bronze_silver_gold",
    default_args=default_args,
    schedule="0 0 * * *",
    catchup=False,
    is_paused_upon_creation=False,
    description="Master orchestrator for Bronze ingestion, Silver transforms, and Gold transforms.",
) as dag:

    start = EmptyOperator(task_id="start")

    trigger_coingecko_bronze_ingest = TriggerDagRunOperator(
        task_id="trigger_coingecko_bronze_ingest",
        trigger_dag_id="ingest_bronze_coingecko_market_data",
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"],
        poke_interval=30,
        execution_timeout=timedelta(minutes=60),
    )

    trigger_fear_greed_bronze_ingest = TriggerDagRunOperator(
        task_id="trigger_fear_greed_bronze_ingest",
        trigger_dag_id="ingest_bronze_fear_greed_index",
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"],
        poke_interval=30,
        execution_timeout=timedelta(minutes=60),
    )

    trigger_onchain_bronze_ingest = TriggerDagRunOperator(
        task_id="trigger_onchain_bronze_ingest",
        trigger_dag_id="ingest_bronze_blockchain_onchain_metrics",
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"],
        poke_interval=30,
        execution_timeout=timedelta(minutes=60),
    )

    trigger_silver_transformations = TriggerDagRunOperator(
        task_id="trigger_silver_transformations",
        trigger_dag_id="transform_silver_crypto_models",
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"],
        poke_interval=30,
        execution_timeout=timedelta(minutes=60),
    )

    trigger_gold_transformations = TriggerDagRunOperator(
        task_id="trigger_gold_transformations",
        trigger_dag_id="transform_gold_crypto_models",
        wait_for_completion=True,
        allowed_states=["success"],
        failed_states=["failed"],
        poke_interval=30,
        execution_timeout=timedelta(minutes=60),
    )

    end = EmptyOperator(task_id="end")

    # All bronze ingestions must complete before Silver starts.
    start >> [
        trigger_coingecko_bronze_ingest,
        trigger_fear_greed_bronze_ingest,
        trigger_onchain_bronze_ingest,
    ]
    [
        trigger_coingecko_bronze_ingest,
        trigger_fear_greed_bronze_ingest,
        trigger_onchain_bronze_ingest,
    ] >> trigger_silver_transformations
    trigger_silver_transformations >> trigger_gold_transformations >> end