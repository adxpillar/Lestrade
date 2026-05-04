-- Phase 1 schema for Lestrade (Form 4 ingestion)
-- Source of truth: docs/DATA_CONTRACT.md

CREATE TABLE IF NOT EXISTS filing_raw (
  accession_number TEXT PRIMARY KEY,
  cik CHAR(10) NOT NULL,
  filing_date DATE NOT NULL,
  accepted_at TIMESTAMPTZ NULL,
  primary_document TEXT NOT NULL,
  raw_xml_s3_uri TEXT NULL,
  raw_xml BYTEA NULL,
  content_sha256 CHAR(64) NOT NULL,
  fetched_at TIMESTAMPTZ NOT NULL,
  http_status SMALLINT NOT NULL,
  fetch_error TEXT NULL
);

CREATE TABLE IF NOT EXISTS filing (
  accession_number TEXT PRIMARY KEY REFERENCES filing_raw(accession_number) ON DELETE CASCADE,
  document_type TEXT NOT NULL,
  is_amendment BOOLEAN NOT NULL,
  schema_version TEXT NULL,
  period_of_report DATE NULL,
  issuer_cik CHAR(10) NOT NULL,
  issuer_name TEXT NULL,
  issuer_ticker TEXT NULL,
  insider_cik CHAR(10) NULL,
  insider_name TEXT NULL,
  insider_title TEXT NULL,
  parsed_at TIMESTAMPTZ NOT NULL,
  parse_error TEXT NULL
);

CREATE TABLE IF NOT EXISTS form4_transaction (
  accession_number TEXT NOT NULL REFERENCES filing(accession_number) ON DELETE CASCADE,
  transaction_index INTEGER NOT NULL,
  transaction_category TEXT NOT NULL,
  security_title TEXT NULL,
  transaction_date DATE NULL,
  transaction_code TEXT NULL,
  acquired_disposed_code CHAR(1) NULL,
  shares NUMERIC(28, 8) NULL,
  price_per_share NUMERIC(28, 8) NULL,
  total_value NUMERIC(28, 4) NULL,
  direct_indirect TEXT NULL,
  footnote_ids TEXT[] NULL,
  PRIMARY KEY (accession_number, transaction_index)
);

CREATE TABLE IF NOT EXISTS ingestion_errors (
  id BIGSERIAL PRIMARY KEY,
  accession_number TEXT NULL,
  stage TEXT NOT NULL,
  error_type TEXT NOT NULL,
  message TEXT NOT NULL,
  payload JSONB NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Recommended indexes (docs/DATA_CONTRACT.md)
CREATE INDEX IF NOT EXISTS idx_form4_tx_by_accession ON form4_transaction(accession_number);
CREATE INDEX IF NOT EXISTS idx_filing_by_issuer_cik ON filing(issuer_cik);
CREATE INDEX IF NOT EXISTS idx_filing_by_issuer_ticker ON filing(issuer_ticker);

CREATE INDEX IF NOT EXISTS idx_form4_tx_by_date ON form4_transaction(transaction_date);
CREATE INDEX IF NOT EXISTS idx_form4_tx_by_code_date ON form4_transaction(transaction_code, transaction_date);

