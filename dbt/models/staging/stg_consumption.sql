-- stg_consumption.sql
-- ────────────────────
-- Cleans the raw electricity consumption table.
--
-- What we fix:
--   1. Drop NULLs and zero/negative values (consumption is always positive)
--   2. Convert UTC → Berlin time
--   3. Extract date parts for easy grouping in mart models

WITH raw AS (
    SELECT
        timestamp,
        value_mwh,
        ingested_at
    FROM raw_energy.consumption
    WHERE
        value_mwh IS NOT NULL
        AND value_mwh > 0
)

SELECT
    timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin' AS timestamp_berlin,
    timestamp AS timestamp_utc,

    -- Pre-extracted date parts so mart queries don't repeat this logic
    (timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin')::date   AS date_berlin,
    EXTRACT(HOUR   FROM timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin') AS hour_of_day,
    EXTRACT(DOW    FROM timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin') AS day_of_week,  -- 0=Sun, 6=Sat
    EXTRACT(MONTH  FROM timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin') AS month_num,
    EXTRACT(YEAR   FROM timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin') AS year_num,

    value_mwh,
    ingested_at

FROM raw
