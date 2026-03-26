# Crypto Data Pipeline (Airflow + Databricks + dbt)

Production-style data engineering project that ingests crypto market, sentiment, and on-chain data into a medallion architecture with automated quality checks.

[![Python](https://img.shields.io/badge/Python-3.10+-blue)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-Container-blue)](https://www.docker.com/)
[![Databricks](https://img.shields.io/badge/Databricks-Processing-orange)](https://databricks.com/)
[![dbt](https://img.shields.io/badge/dbt-Transformations-orange)](https://www.getdbt.com/)
[![Airflow](https://img.shields.io/badge/Airflow-Orchestration-blue)](https://airflow.apache.org/)

## 30-Second Snapshot

If you are a recruiter or hiring manager, here is the value quickly:

- Built an end-to-end data platform with real orchestration, transformations, and quality gates.
- Implemented medallion modeling (Bronze -> Silver -> Gold) on Databricks with dbt.
- Orchestrated independent ingestion + transformation DAGs with an explicit quality DAG.
- Designed analytics-ready gold models for liquidity, dominance, volatility, and sentiment-vs-price behavior.
- Added production-minded practices: idempotent loads, test layering (critical/non-critical), secrets strategy, and containerized local runtime.

## Why This Project Matters For Data Engineering Roles

- Demonstrates platform thinking, not only SQL scripts.
- Shows ability to combine Airflow, dbt, Databricks, and API ingestion in one coherent system.
- Balances data modeling, reliability, and operational concerns.
- Is interview-friendly: architecture, tradeoffs, and quality strategy are explicit and easy to discuss.

## Architecture At A Glance

Sources:

- CoinGecko market data
- Fear and Greed index
- Blockchain on-chain BTC metrics

Processing pattern:

- Bronze: ingestion-aligned raw landing tables
- Silver: cleaned, standardized daily crypto features
- Gold: business-facing analytics models

Orchestration pattern:

- Domain DAGs for ingestion and transformations
- Dedicated quality DAG for dbt tests
- Parameterized test execution by layer (bronze, silver, gold, all)

## Gold Layer Business Outputs

Main gold models answer practical analytics questions:

- gold_liquidity_ranking: Which assets are highly liquid vs potentially fragile?
- gold_btc_dominance: Is BTC gaining or losing market share relative to total market?
- gold_volatility_signal: Which assets are entering high-volatility regimes?
- gold_sentiment_vs_price: How does market sentiment relate to next-day returns?
- gold_market_summary: Daily KPI view for ranking and tracking assets.

## Data Quality Strategy

- dbt tests are organized by impact:
  - Critical tests fail the run when core integrity is broken.
  - Non-critical tests surface warnings without always blocking execution.
- Quality checks can run per layer for targeted debugging and faster iteration.
- The quality DAG is integrated into orchestration so data contracts are enforced regularly, not manually.

## Tech Stack

| Component         | Tooling          | Role                                          |
| ----------------- | ---------------- | --------------------------------------------- |
| Orchestration     | Airflow (Docker) | Scheduling, dependencies, operational control |
| Transformations   | dbt Core         | Medallion modeling and tests                  |
| Compute/Warehouse | Databricks SQL   | Storage and scalable execution                |
| Data Sources      | APIs             | Market, sentiment, on-chain ingestion         |
| Runtime           | Docker Compose   | Reproducible local environment                |

## Fast Start

From airflow-docker:

```bash
docker compose build
docker compose up airflow-init
docker compose up -d
```

Primary DAG flow:

1. orchestrate_crypto_bronze_silver_gold
2. ingest_bronze_coingecko_market_data
3. ingest_bronze_fear_greed_index
4. ingest_bronze_blockchain_onchain_metrics
5. transform_silver_crypto_models
6. transform_gold_crypto_models
7. quality_checks_crypto_data_layers

## Security Notes

- Keep secrets in environment variables or a centralized backend.
- Never commit PAT tokens or local .env files.
- dbt profiles are environment-driven (credentials are not hardcoded in versioned SQL models).

## What I Would Improve Next (Production Gap)

- Add CI pipeline for dbt build/test and DAG validation on pull requests.
- Add runtime observability dashboard (freshness, SLA, and test pass-rate trends).
- Expand contract tests and relationship checks across layers.
- Publish one BI dashboard/storyboard over the gold models for stakeholder consumption.

## Repo Structure

- airflow-docker/: Airflow runtime, DAGs, and Docker setup.
- airflow-docker/dbt/: dbt project (models, tests, macros, profiles).

At runtime, Docker Compose mounts ./dbt into Airflow containers at /opt/airflow/dbt.
