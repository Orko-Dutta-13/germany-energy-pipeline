"""
smard_hook.py
─────────────
A reusable Airflow Hook for the SMARD public API (smard.de).
SMARD is run by Germany's Bundesnetzagentur and publishes electricity
generation, consumption, and price data with no API key required.

How SMARD's API works (two-step):
  Step 1 → GET index URL  → returns a list of available timestamps
  Step 2 → GET data URL   → returns the actual time-series for that timestamp

We abstract both steps here so the Operator doesn't need to know the details.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# ── SMARD filter codes ────────────────────────────────────────────────────────
# Each energy source has a numeric filter code in the SMARD system.
SMARD_FILTERS = {
    "wind_offshore":         1223,
    "wind_onshore":          1224,
    "solar":                 1225,
    "other_renewables":      1226,
    "hydro":                 1227,
    "biomass":               1228,
    "nuclear":               4066,   # zero since April 2023
    "lignite":               4067,   # Braunkohle
    "hard_coal":             4068,   # Steinkohle
    "natural_gas":           4069,
    "pumped_storage":        4070,
    "other_conventional":    4071,
    "consumption":           4359,   # Gesamtverbrauch
    "day_ahead_price":       8784,   # €/MWh
}

SMARD_BASE_URL = "https://www.smard.de/app/chart_data"
REGION = "DE"
RESOLUTION = "quarterhour"   # finest granularity: 15-minute intervals


class SmardHook:
    """
    Fetches electricity data from the SMARD public API.

    Usage:
        hook = SmardHook()
        rows = hook.get_series("solar", date(2024, 1, 15))
        # returns: [{"timestamp": datetime, "value_mwh": float}, ...]
    """

    def __init__(self, base_url: str = SMARD_BASE_URL, retries: int = 3):
        self.base_url = base_url
        self.retries = retries
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "germany-energy-pipeline/1.0"})

    # ── Public method ─────────────────────────────────────────────────────────

    def get_series(self, source: str, target_date) -> list[dict]:
        """
        Fetch all 15-minute intervals for a given energy source on a given date.

        Args:
            source:      Key from SMARD_FILTERS, e.g. "solar"
            target_date: datetime.date object for the day we want

        Returns:
            List of dicts: [{"timestamp": datetime, "value_mwh": float | None}]
        """
        if source not in SMARD_FILTERS:
            raise ValueError(f"Unknown source '{source}'. Valid: {list(SMARD_FILTERS)}")

        filter_id = SMARD_FILTERS[source]
        logger.info(f"Fetching SMARD data — source={source}, date={target_date}")

        # Step 1: get available timestamps from the index
        available_timestamps = self._fetch_index(filter_id)
        if not available_timestamps:
            logger.warning(f"No timestamps available for {source}")
            return []

        # Step 2: find the timestamp bucket that contains our target date
        target_ts_ms = self._date_to_ms(target_date)
        bucket_ts = self._find_bucket(available_timestamps, target_ts_ms)
        if bucket_ts is None:
            logger.warning(f"No data bucket found for {target_date} ({source})")
            return []

        # Step 3: fetch the actual data for that bucket
        raw_series = self._fetch_series(filter_id, bucket_ts)

        # Step 4: filter to only the rows that fall on our target date
        return self._filter_to_date(raw_series, target_date)

    # ── Private helpers ───────────────────────────────────────────────────────

    def _fetch_index(self, filter_id: int) -> list[int]:
        """Returns list of available epoch-millisecond timestamps from SMARD index."""
        url = f"{self.base_url}/{filter_id}/{REGION}/index_{RESOLUTION}.json"
        data = self._get_json(url)
        return data.get("timestamps", []) if data else []

    def _fetch_series(self, filter_id: int, bucket_ts: int) -> list:
        """Returns raw [[timestamp_ms, value], ...] pairs for a bucket."""
        url = (
            f"{self.base_url}/{filter_id}/{REGION}/"
            f"{filter_id}_{REGION}_{RESOLUTION}_{bucket_ts}.json"
        )
        data = self._get_json(url)
        return data.get("series", []) if data else []

    def _get_json(self, url: str) -> Optional[dict]:
        """GET a URL with retry logic. Returns parsed JSON or None on failure."""
        for attempt in range(1, self.retries + 1):
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt}/{self.retries} failed for {url}: {e}")
                if attempt < self.retries:
                    time.sleep(2 ** attempt)   # exponential back-off: 2s, 4s, 8s
        logger.error(f"All {self.retries} attempts failed for {url}")
        return None

    def _date_to_ms(self, target_date) -> int:
        """Convert a date to epoch milliseconds (UTC midnight)."""
        dt = datetime(target_date.year, target_date.month, target_date.day,
                      tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)

    def _find_bucket(self, timestamps: list[int], target_ms: int) -> Optional[int]:
        """
        SMARD groups data into weekly buckets. Find the bucket whose start
        is <= our target date (the closest one before it).
        """
        candidates = [ts for ts in timestamps if ts <= target_ms]
        return max(candidates) if candidates else None

    def _filter_to_date(self, series: list, target_date) -> list[dict]:
        """
        Filter raw [[ts_ms, value], ...] to only rows on target_date.
        Returns clean list of dicts with proper datetime objects.
        """
        results = []
        for ts_ms, value in series:
            if ts_ms is None:
                continue
            dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            if dt.date() == target_date:
                results.append({
                    "timestamp": dt,
                    # SMARD returns MWh already; None means data not yet available
                    "value_mwh": float(value) if value is not None else None,
                })
        logger.info(f"  → {len(results)} rows for {target_date}")
        return results
