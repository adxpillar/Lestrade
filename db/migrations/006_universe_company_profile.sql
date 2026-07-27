-- Lestrade Phase 2 — issuer profile snapshot per universe session (yfinance / etc.).
--
-- Apply with:
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f db/migrations/006_universe_company_profile.sql

BEGIN;

CREATE TABLE IF NOT EXISTS universe_company_profile (
    trading_date DATE NOT NULL,
    ticker TEXT NOT NULL,
    cik CHAR(10),
    sector TEXT,
    industry TEXT,
    market_cap NUMERIC(28, 2),
    sic_code TEXT,
    quote_type TEXT,
    long_name TEXT,
    currency TEXT,
    enrichment_status TEXT NOT NULL
        CHECK (enrichment_status IN ('ok', 'no_data', 'error')),
    error_message TEXT,
    source TEXT NOT NULL DEFAULT 'yfinance',
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    fetched_at_atl_timestamp TIMESTAMP NOT NULL
        GENERATED ALWAYS AS (timezone('America/New_York', fetched_at)) STORED,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at_atl_timestamp TIMESTAMP NOT NULL
        GENERATED ALWAYS AS (timezone('America/New_York', updated_at)) STORED,
    PRIMARY KEY (trading_date, ticker)
);

CREATE INDEX IF NOT EXISTS idx_universe_company_profile_trading_date
    ON universe_company_profile (trading_date);

CREATE INDEX IF NOT EXISTS idx_universe_company_profile_cik
    ON universe_company_profile (cik)
    WHERE cik IS NOT NULL;

ALTER TABLE universe_company_profile ENABLE ROW LEVEL SECURITY;

COMMIT;
