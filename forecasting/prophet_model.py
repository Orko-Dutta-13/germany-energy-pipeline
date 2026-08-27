"""
prophet_model.py
─────────────────
Trains a Prophet model on historical Germany electricity demand
and generates a 7-day forecast.
"""

import logging
import os
from datetime import datetime, timezone

import pandas as pd
import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

MIN_TRAINING_DAYS = 2   # Prophet absolute minimum; accuracy improves at 14+


def get_conn_str() -> str:
    conn = os.environ.get(
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
        "postgresql://airflow:airflow@postgres/airflow"
    )
    return conn.replace("postgresql+psycopg2://", "postgresql://")


def get_engine():
    """SQLAlchemy engine — avoids the pandas DBAPI2 warning."""
    return create_engine(get_conn_str())


# ── Step 1: Load training data ────────────────────────────────────────────────

def load_training_data() -> pd.DataFrame:
    """
    Read daily demand history from the dbt mart table.
    Prophet expects columns named exactly 'ds' (date) and 'y' (value).
    """
    query = """
        SELECT
            date_berlin          AS ds,
            total_mwh            AS y
        FROM analytics.mart_daily_demand
        WHERE
            is_incomplete_day = FALSE
            AND date_berlin < CURRENT_DATE
        ORDER BY date_berlin ASC
    """
    with get_engine().connect() as conn:
        df = pd.read_sql(query, conn, parse_dates=["ds"])

    logger.info(f"Loaded {len(df)} days of training data "
                f"({df['ds'].min().date() if len(df) else 'n/a'} → "
                f"{df['ds'].max().date() if len(df) else 'n/a'})")
    return df


# ── Step 2: Train Prophet model ───────────────────────────────────────────────

def train_model(df: pd.DataFrame):
    from prophet import Prophet

    if len(df) < MIN_TRAINING_DAYS:
        raise ValueError(
            f"Need at least {MIN_TRAINING_DAYS} days of data to train Prophet, "
            f"but only {len(df)} available. "
            "Run the ingestion DAG for a few more days first, or trigger it "
            "manually for past dates using Airflow's backfill feature."
        )

    if len(df) < 14:
        logger.warning(
            f"Only {len(df)} days of data. Forecast will run but accuracy "
            "improves significantly with 2+ weeks of history."
        )

    model = Prophet(
        weekly_seasonality=True,
        yearly_seasonality=True if len(df) >= 365 else False,
        daily_seasonality=False,
        changepoint_prior_scale=0.05,
        interval_width=0.90,
        uncertainty_samples=500,
    )

    model.add_country_holidays(country_name="DE")

    logger.info("Fitting Prophet model...")
    model.fit(df)
    logger.info("Model fitted successfully")
    return model


# ── Step 3: Generate forecast ─────────────────────────────────────────────────

def generate_forecast(model, periods: int = 7) -> pd.DataFrame:
    future = model.make_future_dataframe(periods=periods, freq="D")
    forecast = model.predict(future)

    last_training_date = model.history["ds"].max()
    forecast["is_future"] = forecast["ds"] > last_training_date

    result = forecast[["ds", "yhat", "yhat_lower", "yhat_upper", "is_future"]].copy()
    result["yhat"]       = result["yhat"].round(2)
    result["yhat_lower"] = result["yhat_lower"].round(2)
    result["yhat_upper"] = result["yhat_upper"].round(2)

    future_rows = result[result["is_future"]].shape[0]
    logger.info(f"Generated forecast: {future_rows} future days + "
                f"{len(result) - future_rows} historical fitted values")
    return result


# ── Step 4: Save forecast to Postgres ─────────────────────────────────────────

def ensure_forecast_table(cur):
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE TABLE IF NOT EXISTS analytics.demand_forecast (
            id                  SERIAL PRIMARY KEY,
            forecast_date       DATE NOT NULL,
            predicted_mwh       NUMERIC(12, 2),
            lower_bound_mwh     NUMERIC(12, 2),
            upper_bound_mwh     NUMERIC(12, 2),
            is_future           BOOLEAN NOT NULL,
            model_run_at        TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_forecast_date
            ON analytics.demand_forecast (forecast_date);
    """)


def save_forecast(forecast_df: pd.DataFrame):
    with psycopg2.connect(get_conn_str()) as conn:
        with conn.cursor() as cur:
            ensure_forecast_table(cur)
            cur.execute("DELETE FROM analytics.demand_forecast")

            data = [
                (
                    row["ds"].date(),
                    row["yhat"],
                    row["yhat_lower"],
                    row["yhat_upper"],
                    bool(row["is_future"]),
                )
                for _, row in forecast_df.iterrows()
            ]

            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO analytics.demand_forecast
                   (forecast_date, predicted_mwh, lower_bound_mwh, upper_bound_mwh, is_future)
                   VALUES %s""",
                data,
            )
        conn.commit()
    logger.info(f"Saved {len(data)} rows to analytics.demand_forecast")


# ── Main entry point ───────────────────────────────────────────────────────────

def run_forecast(**context):
    logger.info("Starting Prophet demand forecast pipeline")

    df = load_training_data()

    if df.empty:
        logger.warning("No training data found. Skipping forecast.")
        return

    if len(df) < MIN_TRAINING_DAYS:
        logger.warning(
            f"Only {len(df)} day(s) of data — minimum is {MIN_TRAINING_DAYS}. "
            "Skipping forecast. Run ingestion for more days first."
        )
        return   # exit cleanly, don't fail the task

    model = train_model(df)
    forecast = generate_forecast(model, periods=7)
    save_forecast(forecast)
    logger.info("Forecast pipeline complete ✓")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_forecast()
