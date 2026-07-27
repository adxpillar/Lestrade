-- Lestrade Phase 1 extension — universe snapshots (manual high/low lists).
-- Adds:
-- - security_master: ticker → CIK mapping cache
-- - universe_snapshot: daily "covered tickers" (high/low) stamped by trading date
--
-- Apply with:
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f db/migrations/003_universe_snapshot.sql

BEGIN;

CREATE TABLE IF NOT EXISTS security_master (
    ticker TEXT PRIMARY KEY,
    cik CHAR(10),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    source TEXT NOT NULL DEFAULT 'sec_company_tickers'
);

CREATE INDEX IF NOT EXISTS idx_security_master_cik ON security_master (cik)
    WHERE cik IS NOT NULL;

CREATE TABLE IF NOT EXISTS universe_snapshot (
    trading_date DATE NOT NULL,
    ticker TEXT NOT NULL,
    universe_group TEXT NOT NULL CHECK (universe_group IN ('high', 'low')),
    cik CHAR(10),
    source TEXT NOT NULL DEFAULT 'manual_env',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (trading_date, ticker, universe_group)
);

CREATE INDEX IF NOT EXISTS idx_universe_snapshot_trading_date_group
    ON universe_snapshot (trading_date, universe_group);

CREATE INDEX IF NOT EXISTS idx_universe_snapshot_trading_date_cik
    ON universe_snapshot (trading_date, cik)
    WHERE cik IS NOT NULL;

ALTER TABLE security_master ENABLE ROW LEVEL SECURITY;
ALTER TABLE universe_snapshot ENABLE ROW LEVEL SECURITY;

COMMIT;

