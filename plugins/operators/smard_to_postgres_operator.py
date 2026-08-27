"""
smard_to_postgres_operator.py
──────────────────────────────
A custom Airflow Operator that:
  1. Calls SmardHook to fetch data for one energy source on one date
  2. Writes the rows into the correct Postgres raw table
  3. Logs a record in raw_energy.ingestion_log (so we can track what ran)

Why a custom operator instead of just using PythonOperator?
  → Reusability: you can drop SmardToPostgresOperator anywhere in any DAG.
  → Testability: you can unit-test the operator class without Airflow running.
  → Clean DAGs: the DAG file stays short and readable.
"""

import logging
from datetime import date
from typing import Sequence

import psycopg2
import psycopg2.extras
from airflow.models import BaseOperator

from hooks.smard_hook import SmardHook, SMARD_FILTERS

logger = logging.getLogger(__name__)

# Maps SMARD source names to which Postgres table they belong in
SOURCE_TABLE_MAP = {
    # All generation sources → raw_energy.generation
    "wind_offshore":      ("generation", "source"),
    "wind_onshore":       ("generation", "source"),
    "solar":              ("generation", "source"),
    "other_renewables":   ("generation", "source"),
    "hydro":              ("generation", "source"),
    "biomass":            ("generation", "source"),
    "nuclear":            ("generation", "source"),
    "lignite":            ("generation", "source"),
    "hard_coal":          ("generation", "source"),
    "natural_gas":        ("generation", "source"),
    "pumped_storage":     ("generation", "source"),
    "other_conventional": ("generation", "source"),
    # Consumption → raw_energy.consumption
    "consumption":        ("consumption", None),
    # Prices → raw_energy.prices
    "day_ahead_price":    ("prices", None),
}


class SmardToPostgresOperator(BaseOperator):
    """
    Fetches one SMARD data source for one date and upserts it into Postgres.

    Args:
        source:         Key from SMARD_FILTERS (e.g. "solar", "consumption")
        data_date:      Date to fetch. Defaults to Airflow's logical_date (yesterday).
        postgres_conn:  Postgres connection string (reads from env if not provided)
        replace_existing: If True, delete existing rows for that date before inserting.
                          Keeps the operator idempotent — safe to re-run.
    """

    # Tells Airflow which constructor args support Jinja templating
    template_fields: Sequence[str] = ("data_date",)

    def __init__(
        self,
        source: str,
        data_date: str = "{{ ds }}",   # {{ ds }} = Airflow's execution date (YYYY-MM-DD)
        postgres_conn: str = None,
        replace_existing: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if source not in SMARD_FILTERS:
            raise ValueError(f"Unknown source '{source}'")
        self.source = source
        self.data_date = data_date
        self.postgres_conn = postgres_conn
        self.replace_existing = replace_existing

    # ── Main execution ────────────────────────────────────────────────────────

    def execute(self, context):
        """Called by Airflow when this task runs."""
        import os
        from datetime import datetime

        # Parse the date (Airflow passes ds as "YYYY-MM-DD" string)
        target_date = datetime.strptime(self.data_date, "%Y-%m-%d").date()
        logger.info(f"[SmardToPostgresOperator] source={self.source}, date={target_date}")

        # 1. Fetch data from SMARD
        hook = SmardHook()
        rows = hook.get_series(self.source, target_date)

        if not rows:
            logger.warning(f"No data returned for {self.source} on {target_date}. Skipping.")
            return 0

        # 2. Connect to Postgres
        conn_str = self.postgres_conn or os.environ.get(
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
            "postgresql://airflow:airflow@postgres/airflow"
        )
        # psycopg2 doesn't understand SQLAlchemy prefix
        conn_str = conn_str.replace("postgresql+psycopg2://", "postgresql://")

        with psycopg2.connect(conn_str) as conn:
            with conn.cursor() as cur:
                table_name, _ = SOURCE_TABLE_MAP[self.source]

                # 3. Optionally delete existing rows for idempotency
                if self.replace_existing:
                    self._delete_existing(cur, table_name, target_date)

                # 4. Insert new rows
                rows_inserted = self._insert_rows(cur, table_name, rows)

                # 5. Log to ingestion_log
                self._log_ingestion(cur, target_date, table_name, rows_inserted)

            conn.commit()

        logger.info(f"✓ Inserted {rows_inserted} rows → raw_energy.{table_name}")
        return rows_inserted

    # ── Private helpers ───────────────────────────────────────────────────────

    def _delete_existing(self, cur, table_name: str, target_date: date):
        """Delete rows for this date so re-runs don't create duplicates."""
        if table_name == "generation":
            cur.execute(
                "DELETE FROM raw_energy.generation "
                "WHERE timestamp::date = %s AND source = %s",
                (target_date, self.source)
            )
        elif table_name == "consumption":
            cur.execute(
                "DELETE FROM raw_energy.consumption WHERE timestamp::date = %s",
                (target_date,)
            )
        elif table_name == "prices":
            cur.execute(
                "DELETE FROM raw_energy.prices WHERE timestamp::date = %s",
                (target_date,)
            )
        logger.debug(f"Deleted existing rows for {target_date} in {table_name}")

    def _insert_rows(self, cur, table_name: str, rows: list) -> int:
        """Bulk-insert rows using psycopg2's execute_values for speed."""
        if table_name == "generation":
            data = [(r["timestamp"], self.source, r["value_mwh"]) for r in rows]
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO raw_energy.generation (timestamp, source, value_mwh) VALUES %s",
                data,
            )
        elif table_name == "consumption":
            data = [(r["timestamp"], r["value_mwh"]) for r in rows]
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO raw_energy.consumption (timestamp, value_mwh) VALUES %s",
                data,
            )
        elif table_name == "prices":
            data = [(r["timestamp"], r["value_mwh"]) for r in rows]
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO raw_energy.prices (timestamp, price_eur_mwh) VALUES %s",
                data,
            )
        return len(data)

    def _log_ingestion(self, cur, target_date: date, table_name: str, rows: int):
        """Write a record to ingestion_log for observability."""
        cur.execute(
            """
            INSERT INTO raw_energy.ingestion_log
                (dag_id, data_date, table_name, rows_inserted, status)
            VALUES (%s, %s, %s, %s, 'success')
            """,
            ("smard_ingestion_dag", target_date, table_name, rows),
        )
