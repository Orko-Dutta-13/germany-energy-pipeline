-- stg_prices.sql
-- ───────────────
-- Cleans the raw day-ahead electricity price table.
--
-- Prices CAN be negative (happens when there's too much renewable generation
-- and not enough demand — Germany experiences this regularly).
-- So we only drop NULLs here, not negatives.

WITH raw AS (
    SELECT
        timestamp,
        price_eur_mwh,
        ingested_at
    FROM raw_energy.prices
    WHERE price_eur_mwh IS NOT NULL
)

SELECT
    timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin' AS timestamp_berlin,
    timestamp AS timestamp_utc,
    (timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin')::date AS date_berlin,

    price_eur_mwh,

    -- Flag negative prices — interesting for analysis (renewable surplus events)
    CASE WHEN price_eur_mwh < 0 THEN TRUE ELSE FALSE END AS is_negative_price,

    ingested_at

FROM raw
