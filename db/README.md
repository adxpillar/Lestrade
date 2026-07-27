# Database migrations

**Canonical DDL** for Lestrade Postgres (Supabase, RDS, or local). Apply in order:

| Order | File | Purpose |
|------:|------|---------|
| 1 | `migrations/001_initial_schema.sql` | `filing_raw`, `filing`, `form4_transaction`, `ingestion_errors` |
| 2 | `migrations/002_enable_rls_phase1.sql` | Enable RLS on those tables (Supabase hardening); skip if 001 already matched your process |
| 3 | `migrations/003_universe_snapshot.sql` | `security_master`, `universe_snapshot` |
| 4 | `migrations/004_add_atl_timestamps.sql` | Generated ET (“America/New_York”) timestamps for `TIMESTAMPTZ` fields |
| 5 | `migrations/005_phase2_market_enrichment.sql` | Phase 2 price cache + per-universe-session market context |
| 6 | `migrations/006_universe_company_profile.sql` | Phase 2 issuer profile per session (`universe_company_profile`) |
| 7 | `migrations/007_issuer_sec_profile.sql` | Phase 2 canonical issuer metadata from SEC submissions (`issuer_sec_profile`) |
| 8 | `migrations/008_embedding_index_state.sql` | Phase 3 embedding bookkeeping (`embedding_index_state`) |

Example:

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f db/migrations/001_initial_schema.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f db/migrations/002_enable_rls_phase1.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f db/migrations/003_universe_snapshot.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f db/migrations/004_add_atl_timestamps.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f db/migrations/005_phase2_market_enrichment.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f db/migrations/006_universe_company_profile.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f db/migrations/007_issuer_sec_profile.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f db/migrations/008_embedding_index_state.sql
```

Logical field definitions and ingest behavior are documented in **`docs/DATA_CONTRACT.md`**.
