-- Lestrade Phase 2 — canonical issuer metadata from SEC company submissions (data.sec.gov).
--
-- One row per issuer CIK. SIC and entity fields come from the top-level company submissions JSON,
-- not from Yahoo. Re-fetches overwrite the row (latest SEC snapshot at fetch time).
--
-- Apply with:
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f db/migrations/007_issuer_sec_profile.sql

BEGIN;

CREATE TABLE IF NOT EXISTS issuer_sec_profile (
    cik CHAR(10) NOT NULL,
    entity_name TEXT,
    entity_type TEXT,
    sic_code TEXT,
    sic_description TEXT,
    state_of_incorporation TEXT,
    fiscal_year_end TEXT,
    tickers TEXT[],
    exchanges TEXT[],
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    fetched_at_atl_timestamp TIMESTAMP NOT NULL
        GENERATED ALWAYS AS (timezone('America/New_York', fetched_at)) STORED,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at_atl_timestamp TIMESTAMP NOT NULL
        GENERATED ALWAYS AS (timezone('America/New_York', updated_at)) STORED,
    source TEXT NOT NULL DEFAULT 'sec_submissions',
    enrichment_status TEXT NOT NULL
        CHECK (enrichment_status IN ('ok', 'error')),
    error_message TEXT,
    PRIMARY KEY (cik)
);

CREATE INDEX IF NOT EXISTS idx_issuer_sec_profile_sic ON issuer_sec_profile (sic_code)
    WHERE sic_code IS NOT NULL;

ALTER TABLE issuer_sec_profile ENABLE ROW LEVEL SECURITY;

COMMIT;
