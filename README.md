# ⚡ Germany Energy Data Pipeline

A production-style end-to-end data engineering project that ingests real Germany electricity data from the [SMARD API](https://www.smard.de/) (Bundesnetzagentur), transforms it with dbt, forecasts demand with Prophet, and visualises everything in a Streamlit dashboard — all orchestrated by Apache Airflow running in Docker.

---

## 🏗️ Architecture

```
SMARD API (public, no key)
      │
      ▼
Apache Airflow (Docker)
      │  smard_ingestion_dag  — fetches generation, consumption & prices daily
      │  smard_transformation_dag — runs dbt staging → mart models
      │  smard_forecasting_dag — trains Prophet, writes 7-day forecast
      ▼
PostgreSQL
  ├── raw_energy schema  (raw ingested data)
  ├── staging schema     (dbt staging views)
  └── analytics schema   (dbt mart tables + forecast)
      │
      ▼
Streamlit Dashboard (http://localhost:8501)
```

---

## 🛠️ Tech Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow 2.9.2 |
| Containerisation | Docker + Docker Compose |
| Storage | PostgreSQL 15 |
| Transformation | dbt (dbt-postgres) |
| Forecasting | Prophet (Meta) |
| Dashboard | Streamlit + Plotly |
| CI/CD | GitHub Actions |
| Data source | SMARD API — Bundesnetzagentur |

---

## 📊 Dashboard Features

- 🏭 **Generation Mix** — stacked area chart + pie chart by energy source (wind, solar, coal, gas…)
- 📈 **Daily Demand** — consumption trend + load factor
- 🌱 **Renewable Share** — renewables vs conventional + demand coverage %
- 🔮 **7-Day Forecast** — Prophet predictions with 90% confidence interval

---

## 🚀 Quick Start

### Prerequisites
- Docker Desktop
- Git

### 1. Clone the repo
```bash
git clone https://github.com/Orko-Dutta-13/germany-energy-pipeline.git
cd germany-energy-pipeline
```

### 2. Set up environment
```bash
cp .env.example .env
# Edit .env and set a real Fernet key:
# python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 3. Build and start
```bash
docker-compose build
docker-compose up airflow-init
docker-compose up -d
```

### 4. Access services
| Service | URL |
|---|---|
| Airflow UI | http://localhost:8080 (admin / admin) |
| Dashboard | http://localhost:8501 |

### 5. Trigger the pipeline
In Airflow, enable and trigger these DAGs in order:
1. `smard_ingestion_dag`
2. `smard_transformation_dag`
3. `smard_forecasting_dag`

Or backfill 30 days of history:
```bash
docker exec <airflow-scheduler> airflow dags backfill smard_ingestion_dag \
  --start-date 2026-07-27 --end-date 2026-08-25 -y
```

---

## 📁 Project Structure

```
germany-energy-pipeline/
├── dags/                          # Airflow DAGs
│   ├── ingestion_dag.py           # SMARD API → Postgres
│   ├── transformation_dag.py      # dbt run + test
│   └── forecasting_dag.py        # Prophet training + forecast
├── plugins/
│   ├── hooks/smard_hook.py        # SMARD API client (exponential backoff)
│   └── operators/smard_to_postgres_operator.py
├── dbt/
│   ├── models/staging/            # Staging views (stg_generation, stg_consumption, stg_prices)
│   ├── models/marts/              # Mart tables (generation_by_source, daily_demand, renewable_share)
│   └── macros/generate_schema_name.sql
├── forecasting/
│   └── prophet_model.py           # Prophet train → forecast → save
├── dashboard/
│   ├── app.py                     # Streamlit dashboard
│   └── Dockerfile
├── scripts/
│   └── init_db.sql                # Postgres schema initialisation
├── .github/workflows/ci.yml       # GitHub Actions CI
├── docker-compose.yml
├── Dockerfile                     # Custom Airflow image
└── .env.example
```

---

## 🔄 CI/CD

GitHub Actions runs on every push to `main` or `develop`:
- Python linting (flake8)
- DAG import validation
- dbt schema parse check

---

## 📝 Data Sources

All data is fetched from the **SMARD API** by Bundesnetzagentur (German Federal Network Agency) — publicly available, no API key required.

Energy sources covered: Wind Offshore, Wind Onshore, Solar, Biomass, Run-of-river, Pumped storage, Hard coal, Lignite, Natural gas, Nuclear, and more.

---

## 👤 Author

**Orko Dutta** — [GitHub](https://github.com/Orko-Dutta-13)
