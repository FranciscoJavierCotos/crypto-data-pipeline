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

| Layer / Component       | Technology         | Purpose |
|-------------------------|------------------|---------|
| Orchestration           | Airflow (Docker)  | Schedule & manage tasks |
| Data Processing         | Databricks        | Scalable ETL computations |
| Transformations         | DBT               | Medallion layer transformations |
| Data Source             | CoinGecko API     | Cryptocurrency market data |
| Storage / Architecture  | Medallion Layers  | Bronze, Silver, Gold tables |
| CI / Testing            | Pytest / Unit Tests | Ensure data quality & reliability |

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
