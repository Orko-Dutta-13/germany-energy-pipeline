"""
forecasting_dag.py
──────────────────
DAG: smard_forecasting_dag
Schedule: 08:00 UTC daily (after dbt transformation at 07:00)

Pipeline:
  check_data_availability → run_prophet_forecast → validate_forecast_output
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="smard_forecasting_dag",
    description="Train Prophet model on Germany electricity demand and generate 7-day forecast",
    default_args=default_args,
    schedule_interval="0 8 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["forecasting", "prophet", "germany-energy"],
) as dag:

    # ── Task 1: Check data availability ───────────────────────────────────────
    def check_data_availability(**context):
        import os
        import psycopg2

        conn_str = os.environ.get(
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
            "postgresql://airflow:airflow@postgres/airflow"
        ).replace("postgresql+psycopg2://", "postgresql://")

        with psycopg2.connect(conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) FROM analytics.mart_daily_demand
                    WHERE is_incomplete_day = FALSE
                      AND date_berlin < CURRENT_DATE
                """)
                count = cur.fetchone()[0]

        print(f"Found {count} complete days of demand data")
        if count == 0:
            raise ValueError(
                "No demand data found in analytics.mart_daily_demand. "
                "Run the ingestion and transformation DAGs first."
            )
        print(f"  ✓ {count} days available — proceeding to forecast")

    # ── Task 2: Run Prophet forecast ──────────────────────────────────────────
    def run_prophet_forecast(**context):
        import sys
        sys.path.insert(0, "/opt/airflow/forecasting")
        from prophet_model import run_forecast
        run_forecast(**context)

    # ── Task 3: Validate forecast output ──────────────────────────────────────
    def validate_forecast(**context):
        import os
        import psycopg2

        conn_str = os.environ.get(
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
            "postgresql://airflow:airflow@postgres/airflow"
        ).replace("postgresql+psycopg2://", "postgresql://")

        with psycopg2.connect(conn_str) as conn:
            with conn.cursor() as cur:

                # Check table exists — it won't if forecast was skipped
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = 'analytics'
                          AND table_name = 'demand_forecast'
                    )
                """)
                if not cur.fetchone()[0]:
                    print("demand_forecast table does not exist yet.")
                    print("Forecast was skipped — not enough training data.")
                    print("Backfill the ingestion DAG to accumulate more days.")
                    return  # graceful skip, not a failure

                cur.execute("""
                    SELECT
                        COUNT(*) FILTER (WHERE is_future = TRUE),
                        MIN(predicted_mwh) FILTER (WHERE is_future),
                        MAX(predicted_mwh) FILTER (WHERE is_future),
                        MIN(forecast_date) FILTER (WHERE is_future),
                        MAX(forecast_date) FILTER (WHERE is_future)
                    FROM analytics.demand_forecast
                """)
                future_rows, min_fc, max_fc, first_date, last_date = cur.fetchone()

        print("\n── Forecast validation ──")
        print(f"  Future rows : {future_rows}")
        print(f"  Date range  : {first_date} to {last_date}")

        if future_rows == 0:
            print("  No future rows — forecast skipped (insufficient data).")
            return

        print(f"  MWh range   : {min_fc:,.0f} to {max_fc:,.0f}")

        if future_rows < 7:
            raise ValueError(
                f"Expected 7 future rows, got {future_rows}. Check Prophet output."
            )
        print(f"  ✓ Forecast looks valid ({future_rows} future days generated)")
        if min_fc < 0:
            print(f"  WARNING: Negative forecast values detected ({min_fc:,.0f} MWh).")
            print("  This usually means too little training data. Backfill more days for accuracy.")

    # ── Wire up tasks ─────────────────────────────────────────────────────────
    check_data = PythonOperator(
        task_id="check_data_availability",
        python_callable=check_data_availability,
    )

    run_forecast = PythonOperator(
        task_id="run_prophet_forecast",
        python_callable=run_prophet_forecast,
    )

    validate = PythonOperator(
        task_id="validate_forecast_output",
        python_callable=validate_forecast,
    )

    check_data >> run_forecast >> validate
