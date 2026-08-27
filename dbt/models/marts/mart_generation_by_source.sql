-- mart_generation_by_source.sql
-- ──────────────────────────────
-- Daily generation totals broken out by energy source.
-- This is the main table for the "generation mix" chart in the dashboard.
--
-- Output: one row per (date, source)

WITH daily AS (
    SELECT
        timestamp_berlin::date                          AS date_berlin,
        source,
        source_label,
        is_renewable,
        SUM(value_mwh)                                  AS total_mwh,
        AVG(value_mwh)                                  AS avg_mwh_per_interval,
        COUNT(*)                                        AS interval_count
    FROM {{ ref('stg_generation') }}
    GROUP BY
        timestamp_berlin::date,
        source,
        source_label,
        is_renewable
)

SELECT
    date_berlin,
    source,
    source_label,
    is_renewable,
    ROUND(total_mwh::numeric, 2)            AS total_mwh,
    ROUND(avg_mwh_per_interval::numeric, 2) AS avg_mwh_per_interval,
    interval_count,

    -- Percentage of that day's total generation from this source
    ROUND(
        100.0 * total_mwh / NULLIF(SUM(total_mwh) OVER (PARTITION BY date_berlin), 0),
        2
    ) AS pct_of_daily_generation

FROM daily
ORDER BY date_berlin DESC, total_mwh DESC
