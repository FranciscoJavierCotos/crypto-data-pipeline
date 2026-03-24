ARG AIRFLOW_BASE_IMAGE=apache/airflow:3.1.8
FROM ${AIRFLOW_BASE_IMAGE}

ARG AIRFLOW_VERSION=3.1.8

USER airflow

ENV PATH="/home/airflow/.local/bin:${PATH}"
ENV PIP_DISABLE_PIP_VERSION_CHECK=1

COPY --chown=airflow:0 requirements-airflow.txt /tmp/requirements-airflow.txt

RUN PYTHON_MAJOR_MINOR="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')" \
    && AIRFLOW_CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_MAJOR_MINOR}.txt" \
    && pip install --no-cache-dir --constraint "${AIRFLOW_CONSTRAINTS_URL}" -r /tmp/requirements-airflow.txt
