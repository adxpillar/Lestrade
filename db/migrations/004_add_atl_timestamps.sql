-- Lestrade Phase 1 extension — derived Eastern Time timestamps.
--
-- Adds generated columns that convert TIMESTAMPTZ -> local timestamp in America/New_York.
-- (This is ET with DST; many people refer to this as "EST".)
--
-- Apply with:
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f db/migrations/004_add_atl_timestamps.sql

BEGIN;

-- filing_raw
ALTER TABLE filing_raw
    ADD COLUMN IF NOT EXISTS accepted_at_atl_timestamp TIMESTAMP
        GENERATED ALWAYS AS (timezone('America/New_York', accepted_at)) STORED;

ALTER TABLE filing_raw
    ADD COLUMN IF NOT EXISTS fetched_at_atl_timestamp TIMESTAMP NOT NULL
        GENERATED ALWAYS AS (timezone('America/New_York', fetched_at)) STORED;

-- filing
ALTER TABLE filing
    ADD COLUMN IF NOT EXISTS parsed_at_atl_timestamp TIMESTAMP NOT NULL
        GENERATED ALWAYS AS (timezone('America/New_York', parsed_at)) STORED;

-- ingestion_errors
ALTER TABLE ingestion_errors
    ADD COLUMN IF NOT EXISTS created_at_atl_timestamp TIMESTAMP NOT NULL
        GENERATED ALWAYS AS (timezone('America/New_York', created_at)) STORED;

-- security_master
ALTER TABLE security_master
    ADD COLUMN IF NOT EXISTS updated_at_atl_timestamp TIMESTAMP NOT NULL
        GENERATED ALWAYS AS (timezone('America/New_York', updated_at)) STORED;

-- universe_snapshot
ALTER TABLE universe_snapshot
    ADD COLUMN IF NOT EXISTS created_at_atl_timestamp TIMESTAMP NOT NULL
        GENERATED ALWAYS AS (timezone('America/New_York', created_at)) STORED;

COMMIT;

