-- mart_renewable_share.sql
-- ─────────────────────────
-- Daily renewable energy share — Germany's Energiewende KPI.
-- This is the headline number politicians and journalists quote.
--
-- Output example:
--   date        | total_mwh | renewable_mwh | conventional_mwh | renewable_pct
--   2024-08-25  | 823,100   | 554,500       | 268,600          | 67.4

WITH generation_daily AS (
    SELECT
        date_berlin,
        is_renewable,
        SUM(total_mwh) AS total_mwh
    FROM {{ ref('mart_generation_by_source') }}
    GROUP BY date_berlin, is_renewable
),

pivoted AS (
    SELECT
        date_berlin,
        SUM(total_mwh)                                              AS total_generation_mwh,
        SUM(CASE WHEN is_renewable THEN total_mwh ELSE 0 END)       AS renewable_mwh,
        SUM(CASE WHEN NOT is_renewable THEN total_mwh ELSE 0 END)   AS conventional_mwh
    FROM generation_daily
    GROUP BY date_berlin
),

-- Join with consumption to calculate renewable coverage of actual demand
with_demand AS (
    SELECT
        p.date_berlin,
        p.total_generation_mwh,
        p.renewable_mwh,
        p.conventional_mwh,
        d.total_mwh AS consumption_mwh
    FROM pivoted p
    LEFT JOIN {{ ref('mart_daily_demand') }} d
        ON p.date_berlin = d.date_berlin
)

SELECT
    date_berlin,

    ROUND(total_generation_mwh::numeric, 2) AS total_generation_mwh,
    ROUND(renewable_mwh::numeric, 2)        AS renewable_mwh,
    ROUND(conventional_mwh::numeric, 2)     AS conventional_mwh,
    ROUND(consumption_mwh::numeric, 2)      AS consumption_mwh,

    -- Renewable share of total generation (the standard Energiewende metric)
    ROUND(
        100.0 * renewable_mwh / NULLIF(total_generation_mwh, 0),
        2
    ) AS renewable_pct,

    -- Renewable coverage of demand (can exceed 100% on very windy/sunny days)
    ROUND(
        100.0 * renewable_mwh / NULLIF(consumption_mwh, 0),
        2
    ) AS renewable_demand_coverage_pct

FROM with_demand
ORDER BY date_berlin DESC
