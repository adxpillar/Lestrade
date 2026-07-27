-- Lestrade Phase 2 — market enrichment (daily prices + derived context).
--
-- Apply with:
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f db/migrations/005_phase2_market_enrichment.sql

BEGIN;

-- Daily OHLCV cache for universe tickers (stooq/yfinance/etc).
CREATE TABLE IF NOT EXISTS security_daily_prices (
    ticker TEXT NOT NULL,
    price_date DATE NOT NULL,
    open NUMERIC(18, 6),
    high NUMERIC(18, 6),
    low NUMERIC(18, 6),
    close NUMERIC(18, 6),
    volume BIGINT,
    source TEXT NOT NULL DEFAULT 'stooq',
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ingested_at_atl_timestamp TIMESTAMP NOT NULL
        GENERATED ALWAYS AS (timezone('America/New_York', ingested_at)) STORED,
    PRIMARY KEY (ticker, price_date)
);

CREATE INDEX IF NOT EXISTS idx_security_daily_prices_date
    ON security_daily_prices (price_date);

-- Derived per-session market context for UI + SQL filters.
CREATE TABLE IF NOT EXISTS universe_market_context (
    trading_date DATE NOT NULL,
    universe_group TEXT NOT NULL CHECK (universe_group IN ('high', 'low')),
    ticker TEXT NOT NULL,
    cik CHAR(10),
    close NUMERIC(18, 6),
    volume BIGINT,
    ret_1d NUMERIC(18, 8),
    ret_5d NUMERIC(18, 8),
    ret_20d NUMERIC(18, 8),
    vol_20d NUMERIC(18, 8),
    source TEXT NOT NULL DEFAULT 'stooq',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at_atl_timestamp TIMESTAMP NOT NULL
        GENERATED ALWAYS AS (timezone('America/New_York', created_at)) STORED,
    PRIMARY KEY (trading_date, universe_group, ticker)
);

CREATE INDEX IF NOT EXISTS idx_universe_market_context_trading_date
    ON universe_market_context (trading_date, universe_group);

ALTER TABLE security_daily_prices ENABLE ROW LEVEL SECURITY;
ALTER TABLE universe_market_context ENABLE ROW LEVEL SECURITY;

COMMIT;

