-- stg_generation.sql
-- ───────────────────
-- Cleans the raw electricity generation table.
--
-- What we fix here:
--   1. Remove rows where value_mwh is NULL (sensor outages, missing data)
--   2. Remove negative values (data errors — generation can't be negative)
--   3. Convert UTC timestamp to Berlin local time (CET/CEST)
--   4. Add a human-readable label for each energy source
--   5. Add a flag: is this source renewable or not?
--
-- This model is a VIEW — it always reflects whatever is in raw_energy.generation.
-- No data is duplicated; it's just a clean lens over the raw table.

WITH raw AS (
    SELECT
        timestamp,
        source,
        value_mwh,
        ingested_at
    FROM raw_energy.generation
    WHERE
        value_mwh IS NOT NULL        -- drop missing readings
        AND value_mwh >= 0           -- drop obviously bad data
),

enriched AS (
    SELECT
        -- Convert UTC → Berlin time (handles CET/CEST automatically)
        timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin' AS timestamp_berlin,
        timestamp AS timestamp_utc,

        source,

        -- Human-readable label for dashboards and reports
        CASE source
            WHEN 'wind_offshore'      THEN 'Wind Offshore'
            WHEN 'wind_onshore'       THEN 'Wind Onshore'
            WHEN 'solar'              THEN 'Solar (PV)'
            WHEN 'hydro'              THEN 'Hydropower'
            WHEN 'biomass'            THEN 'Biomass'
            WHEN 'other_renewables'   THEN 'Other Renewables'
            WHEN 'nuclear'            THEN 'Nuclear'
            WHEN 'lignite'            THEN 'Lignite (Brown Coal)'
            WHEN 'hard_coal'          THEN 'Hard Coal'
            WHEN 'natural_gas'        THEN 'Natural Gas'
            WHEN 'pumped_storage'     THEN 'Pumped Storage'
            WHEN 'other_conventional' THEN 'Other Conventional'
            ELSE source
        END AS source_label,

        -- Renewable flag: used to calculate renewable share in marts
        CASE
            WHEN source IN (
                'wind_offshore', 'wind_onshore', 'solar',
                'hydro', 'biomass', 'other_renewables'
            ) THEN TRUE
            ELSE FALSE
        END AS is_renewable,

        value_mwh,
        ingested_at
    FROM raw
)

SELECT * FROM enriched
