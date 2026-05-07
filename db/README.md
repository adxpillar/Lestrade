# Database migrations

**Canonical DDL** for Lestrade Postgres (Supabase, RDS, or local). Apply in order:

| Order | File | Purpose |
|------:|------|---------|
| 1 | `migrations/001_initial_schema.sql` | `filing_raw`, `filing`, `form4_transaction`, `ingestion_errors` |
| 2 | `migrations/002_enable_rls_phase1.sql` | Enable RLS on those tables (Supabase hardening); skip if 001 already matched your process |
| 3 | `migrations/003_universe_snapshot.sql` | `security_master`, `universe_snapshot` |
| 4 | `migrations/004_add_atl_timestamps.sql` | Generated ET (“America/New_York”) timestamps for `TIMESTAMPTZ` fields |

Example:

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f db/migrations/001_initial_schema.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f db/migrations/002_enable_rls_phase1.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f db/migrations/003_universe_snapshot.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f db/migrations/004_add_atl_timestamps.sql
```

Logical field definitions and ingest behavior are documented in **`docs/DATA_CONTRACT.md`**.
