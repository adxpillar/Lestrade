-- Lestrade Phase 1 — initial schema (DATA_CONTRACT.md §4.1–4.3, §8).
-- Creates tables, indexes, and enables RLS (Supabase: anon/authenticated get no policies).
-- Idempotent: safe to re-run if objects already exist (e.g. partial Supabase run).
-- Apply with: psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f db/migrations/001_initial_schema.sql

BEGIN;

CREATE TABLE IF NOT EXISTS filing_raw (
    accession_number TEXT PRIMARY KEY,
    cik CHAR(10) NOT NULL,
    filing_date DATE NOT NULL,
    accepted_at TIMESTAMPTZ,
    primary_document TEXT NOT NULL,
    raw_xml_s3_uri TEXT,
    raw_xml BYTEA,
    content_sha256 CHAR(64) NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    http_status SMALLINT NOT NULL,
    fetch_error TEXT
);

CREATE TABLE IF NOT EXISTS filing (
    accession_number TEXT PRIMARY KEY REFERENCES filing_raw (accession_number),
    document_type TEXT NOT NULL,
    is_amendment BOOLEAN NOT NULL,
    schema_version TEXT,
    period_of_report DATE,
    issuer_cik CHAR(10) NOT NULL,
    issuer_name TEXT,
    issuer_ticker TEXT,
    insider_cik CHAR(10),
    insider_name TEXT,
    insider_title TEXT,
    parsed_at TIMESTAMPTZ NOT NULL,
    parse_error TEXT
);

CREATE TABLE IF NOT EXISTS form4_transaction (
    accession_number TEXT NOT NULL REFERENCES filing (accession_number),
    transaction_index INTEGER NOT NULL,
    transaction_category TEXT NOT NULL,
    security_title TEXT,
    transaction_date DATE,
    transaction_code TEXT,
    acquired_disposed_code CHAR(1),
    shares NUMERIC(28, 8),
    price_per_share NUMERIC(28, 8),
    total_value NUMERIC(28, 4),
    direct_indirect TEXT,
    footnote_ids TEXT[],
    PRIMARY KEY (accession_number, transaction_index)
);

CREATE TABLE IF NOT EXISTS ingestion_errors (
    id BIGSERIAL PRIMARY KEY,
    accession_number TEXT,
    stage TEXT NOT NULL,
    error_type TEXT NOT NULL,
    message TEXT NOT NULL,
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Discovery / backfill: filter by issuer CIK and filing calendar date (§9.2).
CREATE INDEX IF NOT EXISTS idx_filing_raw_cik_filing_date ON filing_raw (cik, filing_date);

-- §4.3 recommended index (columns exist on form4_transaction).
CREATE INDEX IF NOT EXISTS idx_form4_transaction_code_date ON form4_transaction (transaction_code, transaction_date);

-- §4.3 also lists (issuer_cik, transaction_date), (issuer_ticker, transaction_date),
-- (insider_cik, transaction_date); those dimensions live on filing. These pairs
-- support the same query shapes via join (filing ↔ form4_transaction).
CREATE INDEX IF NOT EXISTS idx_filing_issuer_cik_accession ON filing (issuer_cik, accession_number);
CREATE INDEX IF NOT EXISTS idx_filing_issuer_ticker_accession ON filing (issuer_ticker, accession_number)
    WHERE issuer_ticker IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_filing_insider_cik_accession ON filing (insider_cik, accession_number)
    WHERE insider_cik IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_form4_transaction_accession_tx_date ON form4_transaction (accession_number, transaction_date);

CREATE INDEX IF NOT EXISTS idx_ingestion_errors_accession ON ingestion_errors (accession_number)
    WHERE accession_number IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_ingestion_errors_created_at ON ingestion_errors (created_at DESC);

-- PostgREST (anon / authenticated): no policies → default deny. Ingestion uses DB URL or service_role.
ALTER TABLE filing_raw ENABLE ROW LEVEL SECURITY;
ALTER TABLE filing ENABLE ROW LEVEL SECURITY;
ALTER TABLE form4_transaction ENABLE ROW LEVEL SECURITY;
ALTER TABLE ingestion_errors ENABLE ROW LEVEL SECURITY;

COMMIT;
