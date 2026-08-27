-- ── Raw energy schema ─────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS raw_energy;

-- Generation table: one row per timestamp per energy source
CREATE TABLE IF NOT EXISTS raw_energy.generation (
    id              SERIAL PRIMARY KEY,
    timestamp       TIMESTAMPTZ NOT NULL,
    source          VARCHAR(50) NOT NULL,   -- e.g. 'wind_onshore', 'solar', 'coal'
    value_mwh       NUMERIC(12, 2),
    resolution      VARCHAR(10) DEFAULT '15min',
    ingested_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_generation_timestamp ON raw_energy.generation (timestamp);
CREATE INDEX IF NOT EXISTS idx_generation_source    ON raw_energy.generation (source);

-- Consumption table
CREATE TABLE IF NOT EXISTS raw_energy.consumption (
    id              SERIAL PRIMARY KEY,
    timestamp       TIMESTAMPTZ NOT NULL,
    value_mwh       NUMERIC(12, 2),
    resolution      VARCHAR(10) DEFAULT '15min',
    ingested_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_consumption_timestamp ON raw_energy.consumption (timestamp);

-- Day-ahead prices
CREATE TABLE IF NOT EXISTS raw_energy.prices (
    id              SERIAL PRIMARY KEY,
    timestamp       TIMESTAMPTZ NOT NULL,
    price_eur_mwh   NUMERIC(10, 2),
    ingested_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_prices_timestamp ON raw_energy.prices (timestamp);

-- Ingestion log (for idempotency checks in DAGs)
CREATE TABLE IF NOT EXISTS raw_energy.ingestion_log (
    id              SERIAL PRIMARY KEY,
    dag_id          VARCHAR(100),
    data_date       DATE NOT NULL,
    table_name      VARCHAR(100),
    rows_inserted   INTEGER,
    status          VARCHAR(20) DEFAULT 'success',
    run_at          TIMESTAMPTZ DEFAULT NOW()
);
