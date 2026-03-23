# 🚀 Coingecko Data Pipeline

**End-to-end crypto data pipeline fetching CoinGecko data, containerized Airflow, Databricks, DBT, and medallion architecture.**

[![Python](https://img.shields.io/badge/Python-3.10+-blue)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-Container-blue)](https://www.docker.com/)
[![Databricks](https://img.shields.io/badge/Databricks-Processing-orange)](https://databricks.com/)
[![DBT](https://img.shields.io/badge/DBT-Transformations-orange)](https://www.getdbt.com/)
[![Airflow](https://img.shields.io/badge/Airflow-Orchestration-blue)](https://airflow.apache.org/)

---

## 🌟 Project Overview

This is an **ongoing project** building a **production-ready cryptocurrency data pipeline**.  
It fetches, processes, and transforms data from the **CoinGecko API** in a **scalable, reliable, and maintainable** way.

Key goals:

- Scheduled and automated **data ingestion** from CoinGecko
- End-to-end **pipeline design** with modern tools
- **Medallion architecture layers**: Bronze → Silver → Gold
- **Dockerized Airflow** for reproducibility and portability
- Automated **tests and monitoring** for pipeline reliability

---

## 🛠 Tech Stack

| Layer / Component      | Technology          | Purpose                           |
| ---------------------- | ------------------- | --------------------------------- |
| Orchestration          | Airflow (Docker)    | Schedule & manage tasks           |
| Data Processing        | Databricks          | Scalable ETL computations         |
| Transformations        | DBT                 | Medallion layer transformations   |
| Data Source            | CoinGecko API       | Cryptocurrency market data        |
| Storage / Architecture | Medallion Layers    | Bronze, Silver, Gold tables       |
| CI / Testing           | Pytest / Unit Tests | Ensure data quality & reliability |

---

## ✨ Features

- Scheduled **CoinGecko API ingestion**
- **Containerized Airflow** for reproducibility
- Scalable **Databricks processing**
- **DBT transformations** with medallion layers
- **Unit tests & monitoring** for reliability
- **Extensible architecture** for new crypto data sources

---

## 🚧 Project Status

- [x] CoinGecko API ingestion
- [x] Dockerized Airflow setup
- [x] Initial Databricks integration
- [ ] Full DBT transformations to Gold layer
- [ ] Automated testing & monitoring
- [ ] Deployment to production environment

---

## 💡 Motivation

This project demonstrates my ability to **design and implement production-ready data pipelines**.  
It showcases:

- Real-world **data engineering skills**
- Familiarity with **orchestration, ETL, and transformations**
- Knowledge of **best practices**: medallion architecture, containerization, testing
- Ability to build **scalable, maintainable pipelines** from scratch

---

## 🧱 Repository Structure

This repository keeps Airflow and dbt in the same GitHub project:

- `airflow-docker/`: Dockerized Airflow orchestration runtime.
- `airflow-docker/dbt/`: dbt Core transformations and tests for Databricks.

At runtime, Docker Compose mounts `./dbt` into Airflow containers at `/opt/airflow/dbt`.

---

## 🔐 Secure Configuration

1. Copy `airflow-docker/.env.example` to `airflow-docker/.env`.
2. Fill in real Databricks values in `.env` (`DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`, and `AIRFLOW_CONN_DATABRICKS_DEFAULT`).
3. Do not commit `.env` or PAT tokens.

`dbt/profiles.yml` is safe to version because it reads credentials from environment variables only.

---

## ▶️ Run Airflow + dbt

From `airflow-docker/`:

```bash
docker compose build
docker compose up airflow-init
docker compose up -d
```

To verify dbt in the Airflow image:

```bash
docker compose run --rm airflow-cli bash -lc "dbt --version"
docker compose run --rm airflow-cli bash -lc "cd /opt/airflow/dbt && dbt debug --target ${DBT_TARGET:-prod}"
```

Pipeline orchestration is now split into four DAGs:

1. `coingecko_pipeline`
2. `coingecko_silver_transformations_pipeline`
3. `coingecko_gold_transformations_pipeline`
4. `coingecko_quality_pipeline`

`coingecko_pipeline` includes only:

1. `ingest_coingecko_to_bronze`
2. `trigger_quality_after_ingestion`

`coingecko_silver_transformations_pipeline` includes:

1. `run_dbt_silver_transformations`
2. `trigger_quality_after_silver`
3. `trigger_gold_transformations`

`coingecko_gold_transformations_pipeline` includes:

1. `run_dbt_gold_transformations`
2. `trigger_quality_after_gold`

`coingecko_quality_pipeline` runs dbt quality checks and is triggered automatically after both ingestion and transformation DAGs. It can also be triggered independently/manual when needed.

### Trigger Quality DAG With Run Config

Use Airflow UI Trigger DAG and pass JSON config in `dag_run.conf`.

Run all layers (bronze + silver + gold) for a specific batch:

```json
{
  "layer": "all",
  "batch_id": "<your_ingestion_batch_id>",
  "fail_on_non_critical": false
}
```

Run only silver checks:

```json
{
  "layer": "silver",
  "fail_on_non_critical": true
}
```

Supported `layer` values are `bronze`, `silver`, `gold`, and `all`.
For `bronze` and `all`, `batch_id` is required because bronze tests are batch-scoped.
