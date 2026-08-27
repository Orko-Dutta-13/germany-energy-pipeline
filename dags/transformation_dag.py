"""
transformation_dag.py
──────────────────────
DAG: smard_transformation_dag
Schedule: daily at 07:00 UTC (1 hour after ingestion DAG finishes)

What this DAG does:
  Runs dbt to transform raw Postgres data → clean analytics tables.
  Uses BashOperator to execute dbt CLI commands inside the container.

Task order:
  dbt_deps → dbt_run_staging → dbt_test_staging → dbt_run_marts → dbt_test_marts

Why separate ingestion and transformation DAGs?
  → If ingestion fails, transformation won't run on bad data.
  → You can re-run transformation independently without re-fetching from SMARD.
  → Clean separation of concerns: one DAG = one responsibility.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}

# dbt project lives at /opt/airflow/dbt inside the container
DBT_DIR = "/opt/airflow/dbt"
DBT_CMD = f"cd {DBT_DIR} && dbt"

with DAG(
    dag_id="smard_transformation_dag",
    description="Run dbt models to transform raw energy data into analytics tables",
    default_args=default_args,
    schedule_interval="0 7 * * *",    # 07:00 UTC — after ingestion at 06:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["transformation", "dbt", "germany-energy"],
) as dag:

    # Step 1: install dbt packages (e.g. dbt-utils if we add it later)
    # Safe to run every time — skips if already installed
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"{DBT_CMD} deps --profiles-dir {DBT_DIR}",
    )

    # Step 2: run staging models (views over raw tables)
    # --select staging runs only models in the staging/ folder
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"{DBT_CMD} run --select staging --profiles-dir {DBT_DIR}",
    )

    # Step 3: test staging models
    # Runs all tests defined in staging/schema.yml
    # If a test fails, this task goes red and marts won't run
    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"{DBT_CMD} test --select staging --profiles-dir {DBT_DIR}",
    )

    # Step 4: run mart models (aggregated tables)
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"{DBT_CMD} run --select marts --profiles-dir {DBT_DIR}",
    )

    # Step 5: test mart models
    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=f"{DBT_CMD} test --select marts --profiles-dir {DBT_DIR}",
    )

    # ── Pipeline order ────────────────────────────────────────────────────────
    # Each step must complete successfully before the next one starts
    dbt_deps >> dbt_run_staging >> dbt_test_staging >> dbt_run_marts >> dbt_test_marts
