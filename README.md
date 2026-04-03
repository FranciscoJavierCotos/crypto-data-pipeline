# Crypto Data Pipeline

This project is a production-style data engineering pipeline that collects crypto data every day and turns it into analytics-ready tables.

It combines Airflow, Databricks, and dbt to ingest data from multiple APIs, standardize it through a medallion model (bronze, silver, gold), and run quality checks before publishing outputs.

## Why This Project Has Value

This project demonstrates practical data engineering skills that companies need:

- Building reliable daily ingestion pipelines.
- Combining orchestration, transformation, and testing in one system.
- Delivering business-facing datasets, not only raw data.
- Applying production practices like retries, idempotent loads, and quality gates.

## What It Does (Simple View)

Every day, the pipeline:

1. Ingests crypto market, sentiment, and on-chain data.
2. Stores raw data in bronze tables.
3. Transforms it into cleaned silver models.
4. Builds gold models for analytics and reporting.
5. Runs data quality tests to validate outputs.

## Data Sources

- CoinGecko (market data)
- Alternative.me Fear and Greed Index (sentiment)
- Blockchain.com Charts API (on-chain BTC metrics)

## Daily Orchestration Flow

The main orchestrator is `master_pipeline`.

Flow:

1. `coingecko_bronze_ingest`, `fear_greed_bronze_ingest`, `onchain_bronze_ingest` (parallel)
2. `silver_transform`
3. `gold_transform`
4. `data_quality`

Schedule:

- Runs daily at 00:05 UTC.
- Catchup is disabled.

## Business-Oriented Outputs

Gold models support use cases such as:

- Daily market summary and rankings.
- BTC dominance analysis.
- Volatility monitoring.
- Sentiment vs price behavior.
- Liquidity-focused views for asset comparison.

## Reliability and Quality

- Airflow retries with exponential backoff.
- Idempotent bronze load patterns to avoid duplicate corruption.
- dbt tests separated into critical and non-critical checks.
- Dedicated quality DAG to enforce data contracts regularly.

## Tech Stack

| Component       | Tooling                 | Purpose                              |
| --------------- | ----------------------- | ------------------------------------ |
| Orchestration   | Apache Airflow (Docker) | Scheduling and dependency management |
| Transformations | dbt Core                | Modeling and testing                 |
| Warehouse       | Databricks SQL          | Storage and compute                  |
| Runtime         | Docker Compose          | Local reproducible environment       |

## Quick Start

From `airflow-docker`:

```bash
docker compose build
docker compose up airflow-init
docker compose up -d
```

## Repository Layout

- `airflow-docker/dags/`: ingestion, transformation, orchestration, and maintenance DAGs.
- `airflow-docker/dbt/`: dbt models, tests, macros, and profiles.

## Security Notes

- Store credentials in environment variables or Airflow connections.
- Do not commit real tokens or `.env` secrets.
