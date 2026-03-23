FROM apache/airflow:3.1.8

USER airflow

ENV PATH="/home/airflow/.local/bin:${PATH}"

RUN pip install --no-cache-dir \
    "dbt-databricks" \
    "apache-airflow-providers-databricks"
