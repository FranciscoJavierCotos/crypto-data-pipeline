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
3. Set strong values for `POSTGRES_PASSWORD`, `REDIS_PASSWORD`, `_AIRFLOW_WWW_USER_PASSWORD`, `AIRFLOW__CORE__FERNET_KEY`, and `AIRFLOW__API_AUTH__JWT_SECRET`.
4. Keep `AIRFLOW__CORE__AUTH_MANAGER` on FAB for production.
5. Do not commit `.env` or PAT tokens.

### Centralized Secrets Backend (Production)

For production, prefer external secret stores over local `.env` secrets.

Set in `.env`:

```bash
AIRFLOW__SECRETS__BACKEND=airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
AIRFLOW__SECRETS__BACKEND_KWARGS={"connections_prefix":"airflow/connections","variables_prefix":"airflow/variables","region_name":"us-east-1"}
```

With a secrets backend enabled, keep runtime secrets in the secret manager and avoid storing credentials in `airflow.cfg`.

`dbt/profiles.yml` is safe to version because it reads credentials from environment variables only.

---

## ▶️ Run Airflow + dbt

From `airflow-docker/`:

```bash
docker compose build
docker compose up airflow-init
docker compose up -d
```

Pipeline orchestration uses role-based DAG names:

1. `orchestrate_crypto_bronze_silver_gold`
2. `ingest_bronze_coingecko_market_data`
3. `ingest_bronze_fear_greed_index`
4. `ingest_bronze_blockchain_onchain_metrics`
5. `transform_silver_crypto_models`
6. `transform_gold_crypto_models`
7. `quality_checks_crypto_data_layers`

`ingest_bronze_coingecko_market_data` includes:

1. `ingest_coingecko_to_bronze`
2. `trigger_quality_after_ingestion`

`transform_silver_crypto_models` includes:

1. `run_dbt_silver_transformations`
2. `trigger_quality_after_silver`

`transform_gold_crypto_models` includes:

1. `run_dbt_gold_transformations`
2. `trigger_quality_after_gold`

`quality_checks_crypto_data_layers` runs dbt quality checks and is triggered automatically after both ingestion and transformation DAGs. It can also be triggered independently/manual when needed.
