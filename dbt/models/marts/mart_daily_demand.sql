-- mart_daily_demand.sql
-- ──────────────────────
-- Daily electricity demand summary — this is the table Prophet will forecast.
-- One row per day with total consumption + breakdown by time-of-day patterns.
--
-- Output example:
--   date        | total_mwh | peak_mwh | min_mwh | avg_hourly_mwh | day_of_week | is_weekend
--   2024-08-25  | 1,382,400 | 72,400   | 41,200  | 57,600         | 0 (Sun)     | true

WITH hourly AS (
    -- First aggregate 15-min intervals → hourly (cleaner for forecasting)
    SELECT
        date_berlin,
        hour_of_day,
        day_of_week,
        month_num,
        year_num,
        SUM(value_mwh)  AS hourly_mwh
    FROM {{ ref('stg_consumption') }}
    GROUP BY date_berlin, hour_of_day, day_of_week, month_num, year_num
),

daily AS (
    SELECT
        date_berlin,
        -- Take first value of these (they're constant within a day)
        MAX(day_of_week) AS day_of_week,
        MAX(month_num)   AS month_num,
        MAX(year_num)    AS year_num,

        SUM(hourly_mwh)  AS total_mwh,
        MAX(hourly_mwh)  AS peak_mwh,        -- highest hour of the day
        MIN(hourly_mwh)  AS min_mwh,         -- lowest hour (usually 3-4am)
        AVG(hourly_mwh)  AS avg_hourly_mwh,
        COUNT(*)         AS hours_with_data  -- should be 24; <24 means missing data
    FROM hourly
    GROUP BY date_berlin
)

SELECT
    date_berlin,
    day_of_week,
    -- Weekend flag — consumption patterns differ significantly on weekends
    CASE WHEN day_of_week IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    month_num,
    year_num,

    ROUND(total_mwh::numeric, 2)       AS total_mwh,
    ROUND(peak_mwh::numeric, 2)        AS peak_mwh,
    ROUND(min_mwh::numeric, 2)         AS min_mwh,
    ROUND(avg_hourly_mwh::numeric, 2)  AS avg_hourly_mwh,

    -- Load factor: how flat/peaky the day was (1.0 = perfectly flat)
    ROUND(
        (avg_hourly_mwh / NULLIF(peak_mwh, 0))::numeric, 4
    ) AS load_factor,

    hours_with_data,

    -- Flag days with incomplete data so forecast model can exclude them
    CASE WHEN hours_with_data < 24 THEN TRUE ELSE FALSE END AS is_incomplete_day

FROM daily
ORDER BY date_berlin DESC
