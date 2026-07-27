-- Phase 1 tables — Row Level Security (Supabase / PostgREST hardening)
--
-- New databases: use db/migrations/001_initial_schema.sql only — it already enables RLS.
--
-- Run this file only if tables were created without RLS (e.g. old 001 before RLS). Safe to run more than once.
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f db/migrations/002_enable_rls_phase1.sql
--
-- Intent:
-- - These tables are written by ingestion (Airflow / Python) via the database URL,
--   not by end users through the Supabase Data API.
-- - With RLS enabled and no policies for anon/authenticated, PostgREST clients using
--   the anon or authenticated keys cannot read or write rows (default deny).
-- - Backend jobs should connect as the Postgres role (dashboard "Database" settings)
--   or use the service_role key where applicable; those roles bypass RLS in Supabase.
--
-- If you later expose curated reads to logged-in users, add explicit policies and
-- tests; do not widen access without a user-ownership column in the contract.

BEGIN;

ALTER TABLE filing_raw ENABLE ROW LEVEL SECURITY;
ALTER TABLE filing ENABLE ROW LEVEL SECURITY;
ALTER TABLE form4_transaction ENABLE ROW LEVEL SECURITY;
ALTER TABLE ingestion_errors ENABLE ROW LEVEL SECURITY;

COMMIT;
