# Data Engineering Portfolio
### Ahmad Bakundi | MSc Data Science & AI | Middlesex University

A production-grade data engineering portfolio built from scratch using industry-standard tools.

---

## 🛠️ Tech Stack
- **Database:** PostgreSQL 15 (Docker)
- **ETL:** Python, Pandas, SQLAlchemy
- **Cloud:** GCP, BigQuery (coming Week 2)
- **Orchestration:** Apache Airflow (coming Week 2)
- **Transformation:** dbt (coming Week 3)
- **Processing:** Apache Spark (coming Week 4)
- **Streaming:** Apache Kafka (coming Week 4)

---

## 📁 Project Structure

### Week 1 — Foundations: Postgres + Python ETL
| File | Description |
|------|-------------|
| `week1/postgres/setup_db.sql` | Database schema setup |
| `week1/postgres/generate_data.py` | Generates 10,000 synthetic Olist orders |
| `week1/postgres/generate_all_tables.py` | Generates customers, products, payments data |
| `week1/postgres/load_to_postgres.py` | Python ETL — loads CSV into Postgres |
| `week1/postgres/analytical_queries.sql` | Multi-table JOIN queries for business insights |

---

## 📊 Dataset
Synthetic e-commerce dataset modelled after the Olist Brazilian E-Commerce dataset.
- 10,000 orders
- 500 customers across 7 cities
- 200 products across 7 categories
- 10,000 payment records

---

## 🚀 How to Run
```bash
# Start Postgres
docker run -d --name postgres-de \
  -e POSTGRES_USER=ahmad \
  -e POSTGRES_PASSWORD=ahmad123 \
  -e POSTGRES_DB=olist_db \
  -p 5432:5432 postgres:15

# Generate and load data
python3 week1/postgres/generate_all_tables.py

# Connect and query
docker exec -it postgres-de psql -U ahmad -d olist_db
```

---

## 📈 Progress
- [x] Week 1 — Postgres, Python ETL, SQL Analytics
- [ ] Week 2 — Docker Compose, Airflow, GCP
- [ ] Week 3 — dbt, BigQuery, Looker Studio
- [ ] Week 4 — Spark, Kafka, Data Quality

### Week 2 — Orchestration: Apache Airflow + GCP BigQuery
| File | Description |
|------|-------------|
| `week2/airflow/docker-compose.yml` | Airflow stack — webserver, scheduler, postgres |
| `week2/airflow/dags/olist_etl_dag.py` | Full ETL DAG with data quality checks and BigQuery load |

**Pipeline Flow:**
- [x] Apache Airflow orchestration
- [x] Data quality branching
- [x] GCP BigQuery cloud data warehouse

### Week 2 Day 5 — Looker Studio Dashboard
Live business intelligence dashboard built on top of dbt mart models in BigQuery.

**[View Live Dashboard](https://datastudio.google.com/reporting/aeefb530-f446-448e-963b-70de9d5a871c)**

Charts included:
- Total Revenue scorecard
- Total Orders scorecard
- Revenue by City bar chart
- Orders by Payment Type pie chart
- Monthly Revenue Trend line chart
- City filter for interactive slicing

## CI/CD Status
![Pipeline Tests](https://github.com/ahmad1bakundi-ops/data-engineering-portfolio/actions/workflows/pipeline_tests.yml/badge.svg)
