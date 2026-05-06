# Data Engineering Portfolio
### Ahmad Bakundi | MSc Data Science & AI | Middlesex University

[![Pipeline Tests](https://github.com/ahmad1bakundi-ops/data-engineering-portfolio/actions/workflows/pipeline_tests.yml/badge.svg)](https://github.com/ahmad1bakundi-ops/data-engineering-portfolio/actions)
[![Python](https://img.shields.io/badge/Python-3.12-blue)](https://python.org)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.8-green)](https://airflow.apache.org)
[![BigQuery](https://img.shields.io/badge/BigQuery-GCP-orange)](https://cloud.google.com/bigquery)
[![dbt](https://img.shields.io/badge/dbt-1.11-red)](https://getdbt.com)

---

## About Me
Data Engineer with a background in Civil Engineering and an MSc in Data Science & AI. I build production-grade data pipelines that move data from raw sources to business insights — using the same tools as Spotify, Airbnb, and Uber.

**Target roles:** Data Engineer | Analytics Engineer | AI/LLM Engineer
**Location:** UAE (open to remote global roles)
**GitHub:** [ahmad1bakundi-ops](https://github.com/ahmad1bakundi-ops)

---

## Tech Stack

| Category | Tools |
|----------|-------|
| Languages | Python, SQL |
| Orchestration | Apache Airflow |
| Databases | PostgreSQL, GCP BigQuery |
| Transformation | dbt, Pandas, PySpark |
| Streaming | Apache Kafka |
| Cloud | GCP (BigQuery, Dataproc, GCS) |
| Containerisation | Docker, Docker Compose |
| CI/CD | GitHub Actions |
| Visualisation | Looker Studio |
| Version Control | Git, GitHub |

---

## Projects

### Project 1 — Olist E-Commerce Data Pipeline
**A full end-to-end batch pipeline processing 12,000+ e-commerce orders**

**Live Dashboard:** [View on Looker Studio](https://datastudio.google.com/reporting/aeefb530-f446-448e-963b-70de9d5a871c)

**Pipeline Architecture:**
**Key Features:**
- Automated daily ingestion with Apache Airflow
- Data quality branching — pipeline halts on bad data
- dbt staging and mart models with 6 automated tests
- PySpark window functions — customer ranking, running totals
- Kafka real-time streaming simulation
- Interactive dashboard with city-level revenue breakdown

**Tech:** Python, Pandas, PySpark, Airflow, PostgreSQL, BigQuery, dbt, Kafka, Docker

---

### Project 2 — Nigerian Economic Data Pipeline
**A weekly pipeline ingesting 15 years of real Nigerian economic data from the World Bank API**

**Live Dashboard:** [View on Looker Studio](https://datastudio.google.com/reporting/7283a1b8-c753-4494-81f7-dd13c646387c)

**Pipeline Architecture:**
**Key Insights from the Data:**
- GDP growth went negative in 2016 (oil crash) and 2020 (COVID-19)
- Inflation reached crisis level at 33% in 2024
- GDP per capita fell from $3,088 (2014) to $1,084 (2024)
- Hardship index peaked in 2020 and 2024

**Tech:** World Bank API (wbgapi), Python, Airflow, BigQuery, dbt, Looker Studio

---

## Repository Structure
---

## CI/CD
Every push to this repository automatically runs 9 pipeline tests via GitHub Actions.
All tests currently passing ✅

---

## Contact
- **LinkedIn:** [Connect with me](#)
- **Email:** Available on request
- **Location:** Dubai, UAE | Open to remote roles globally
