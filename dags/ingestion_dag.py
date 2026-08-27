"""
ingestion_dag.py
─────────────────
DAG: smard_ingestion_dag
Schedule: daily at 06:00 UTC (SMARD publishes previous day's data by ~05:00)

What this DAG does:
  For each energy source, it runs one SmardToPostgresOperator task.
  Tasks for different sources run IN PARALLEL (max 4 at a time) to speed things up.
  At the end, a final task checks the ingestion_log to verify everything landed.

Why 06:00 UTC?
  SMARD typically makes the previous day's complete data available by 05:00 UTC.
  We add an hour of buffer so we don't race the API.

DAG structure (all tasks run in parallel, then validate):
  ingest_wind_offshore ──┐
  ingest_wind_onshore  ──┤
  ingest_solar         ──┤
  ingest_other_ren     ──┤
  ingest_hydro         ──┤→ validate_ingestion
  ingest_biomass       ──┤
  ingest_lignite       ──┤
  ingest_hard_coal     ──┤
  ingest_natural_gas   ──┤
  ingest_consumption   ──┤
  ingest_prices        ──┘
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Our custom operator (Airflow finds it because ./plugins is on PYTHONPATH)
from operators.smard_to_postgres_operator import SmardToPostgresOperator

# ── Default arguments applied to every task in this DAG ──────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,        # don't wait for yesterday's run to succeed
    "email_on_failure": False,       # set to True + add email in production
    "email_on_retry": False,
    "retries": 2,                    # retry failed tasks twice
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="smard_ingestion_dag",
    description="Daily ingestion of Germany electricity data from SMARD API → Postgres",
    default_args=default_args,
    schedule_interval="0 6 * * *",          # cron: 06:00 UTC every day
    start_date=datetime(2024, 1, 1),         # backfill available from this date
    catchup=False,                           # don't backfill on first deploy
    max_active_runs=1,                       # only one run at a time
    max_active_tasks=4,                      # max 4 tasks running in parallel
    tags=["ingestion", "smard", "germany-energy"],
) as dag:

    # ── Ingestion tasks (one per energy source) ───────────────────────────────
    # {{ ds }} is Airflow's template variable — it resolves to the execution date
    # as "YYYY-MM-DD". So if the DAG runs on 2024-06-15, it fetches data for
    # 2024-06-14 (yesterday), which is what we want.

    sources = {
        "wind_offshore":      "ingest_wind_offshore",
        "wind_onshore":       "ingest_wind_onshore",
        "solar":              "ingest_solar",
        "other_renewables":   "ingest_other_renewables",
        "hydro":              "ingest_hydro",
        "biomass":            "ingest_biomass",
        "lignite":            "ingest_lignite",
        "hard_coal":          "ingest_hard_coal",
        "natural_gas":        "ingest_natural_gas",
        "consumption":        "ingest_consumption",
        "day_ahead_price":    "ingest_prices",
    }

    ingest_tasks = []
    for source, task_id in sources.items():
        task = SmardToPostgresOperator(
            task_id=task_id,
            source=source,
            data_date="{{ ds }}",       # Airflow fills this in at runtime
            replace_existing=True,      # idempotent: safe to re-run
        )
        ingest_tasks.append(task)

    # ── Validation task ───────────────────────────────────────────────────────
    # Runs AFTER all ingestion tasks. Queries ingestion_log and fails loudly
    # if any source has no rows — better to catch this in Airflow than in dbt.

    def validate_ingestion(ds, **context):
        """
        Check that today's ingestion actually loaded rows.
        Raises an exception (which fails the task) if any source is missing.
        """
        import os
        import psycopg2

        conn_str = os.environ.get(
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
            "postgresql://airflow:airflow@postgres/airflow"
        ).replace("postgresql+psycopg2://", "postgresql://")

        with psycopg2.connect(conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name, SUM(rows_inserted) as total_rows
                    FROM raw_energy.ingestion_log
                    WHERE data_date = %s AND status = 'success'
                    GROUP BY table_name
                    """,
                    (ds,)
                )
                results = cur.fetchall()

        if not results:
            raise ValueError(f"Validation failed: no ingestion records found for {ds}")

        print(f"\n── Ingestion summary for {ds} ──")
        for table, rows in results:
            print(f"  raw_energy.{table}: {rows} rows")
            if rows == 0:
                raise ValueError(f"Table {table} has 0 rows for {ds} — check SMARD API!")
        print("✓ All tables have data\n")

    validate = PythonOperator(
        task_id="validate_ingestion",
        python_callable=validate_ingestion,
    )

    # ── Set dependency: all ingest tasks must finish before validate ──────────
    ingest_tasks >> validate
