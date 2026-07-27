-- Lestrade Phase 3 — embedding index bookkeeping (Postgres source of truth for Chroma sync).
--
-- Apply with:
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f db/migrations/008_embedding_index_state.sql

BEGIN;

CREATE TABLE IF NOT EXISTS embedding_index_state (
    accession_number TEXT NOT NULL,
    transaction_index INTEGER NOT NULL,
    model_id TEXT NOT NULL,
    content_hash CHAR(64) NOT NULL,
    chroma_collection TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('ok', 'error')),
    error_message TEXT,
    embedded_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (accession_number, transaction_index, model_id)
);

CREATE INDEX IF NOT EXISTS idx_embedding_index_state_model_status
    ON embedding_index_state (model_id, status);

CREATE INDEX IF NOT EXISTS idx_embedding_index_state_content_hash
    ON embedding_index_state (model_id, content_hash);

ALTER TABLE embedding_index_state ENABLE ROW LEVEL SECURITY;

COMMIT;
