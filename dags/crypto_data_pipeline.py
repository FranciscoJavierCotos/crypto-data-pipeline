from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from pipeline_callbacks import task_failure_alert, sla_miss_alert


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 21),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": task_failure_alert,
    "sla": timedelta(hours=2),
}


with DAG(
    dag_id="crypto_data_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    description="Master DAG orchestrating Bronze -> Silver -> Gold pipelines.",
    sla_miss_callback=sla_miss_alert,
) as dag:

    start = EmptyOperator(task_id="start")

    trigger_coingecko_bronze_ingest = TriggerDagRunOperator(
        task_id="trigger_coingecko_bronze_ingest",
        trigger_dag_id="coingecko_pipeline",
        wait_for_completion=True,
        poke_interval=30,
    )

    trigger_fear_greed_bronze_ingest = TriggerDagRunOperator(
        task_id="trigger_fear_greed_bronze_ingest",
        trigger_dag_id="fear_greed_pipeline",
        wait_for_completion=True,
        poke_interval=30,
    )

    trigger_silver_transformations = TriggerDagRunOperator(
        task_id="trigger_silver_transformations",
        trigger_dag_id="coingecko_silver_transformations_pipeline",
        wait_for_completion=True,
        poke_interval=30,
    )

    trigger_gold_transformations = TriggerDagRunOperator(
        task_id="trigger_gold_transformations",
        trigger_dag_id="coingecko_gold_transformations_pipeline",
        wait_for_completion=True,
        poke_interval=30,
    )

    end = EmptyOperator(task_id="end")

    # Both bronze ingestions must complete before Silver starts.
    start >> [trigger_coingecko_bronze_ingest, trigger_fear_greed_bronze_ingest]
    [trigger_coingecko_bronze_ingest, trigger_fear_greed_bronze_ingest] >> trigger_silver_transformations
    trigger_silver_transformations >> trigger_gold_transformations >> end