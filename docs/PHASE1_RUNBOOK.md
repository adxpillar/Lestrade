# Phase 1 — Operations runbook, monitoring, and validation

This document complements **[DATA_CONTRACT.md](./DATA_CONTRACT.md)** (normative field rules) with **how to run**, **what to watch**, and **how to validate** the ingestion stack in production-like environments (Docker Airflow + Postgres, or MWAA + RDS).

---

## 1. Prerequisites

| Requirement | Notes |
|-------------|--------|
| Postgres | Apply **`db/migrations/`** in order through at least **`003`** for Phase 1 filing + universe tables; **`004`**–**`008`** add ET timestamps, Phase 2 enrichment tables, and Phase 3 **`embedding_index_state`** (see **`db/README.md`**). |
| Airflow Connection | **`lestrade_rds`** → Supabase/RDS URI with `sslmode=require` where needed. |
| SEC | **`EDGAR_APP_NAME`** + **`EDGAR_CONTACT_EMAIL`** (or Variables `edgar_app_name` / `edgar_contact_email`). |
| Airflow pool | Create pool **`edgar_http`** with a **low slot count** (e.g. 4–6) for any DAG that calls SEC or Stooq. |

---

## 2. DAG order (typical day)

1. **`universe_snapshot`** — Drop exactly one highs + one lows CSV in the configured Barchart directory; filenames must agree on **`MM-DD-YYYY`**. Optional **`LESTRADE_UNIVERSE_TRADING_DATE`** (ISO) must match when set. Archives CSVs on success.
2. **`edgar_daily_incremental`** — Set **`LESTRADE_UNIVERSE_TRADING_DATE`** to the same session; ingests new Form 4s for CIKs in that snapshot only.
3. **`edgar_backfill_on_entry`** — Optional; backfills **new entrants** vs prior snapshot over **`LESTRADE_ENTRY_BACKFILL_DAYS`** (long-running; see timeouts below).
4. **`edgar_backfill`** — Manual / parameterized recovery (`start_date`, `end_date`, CIK list, etc.).
5. **`enrich_universe_market_context`** — Phase 2 market metrics; uses cached Stooq daily prices + **`LESTRADE_UNIVERSE_TRADING_DATE`**.
6. **`enrich_universe_company_profile`** — Phase 2 issuer profile (Yahoo Finance / **`yfinance`**); requires migration **`006`** and package extra **`[enrich]`** (see **`docs/PHASE2_DATA.md`**).
7. **`enrich_issuer_sec_profile`** — Phase 2 **SEC** issuer metadata (SIC, entity name) from **`data.sec.gov`** submissions JSON; migration **`007`**; scope **`LESTRADE_ISSUER_SEC_SCOPE`** (`universe` \| `security_master`).
8. **`embed_form4_transactions`** — Phase 3 transaction embeddings (MiniLM then Voyage into separate Chroma collections); migration **`008`**; package extra **`[embed]`** (see **`docs/PHASE3_ARCHITECTURE.md`**).

**Stamp rule:** Incremental and entrant DAGs use **`LESTRADE_UNIVERSE_TRADING_DATE`**, not `max(trading_date)` from Postgres, so reruns stay tied to the intended session.

---

## 3. Environment variables (reference)

| Variable | Used by | Purpose |
|----------|---------|---------|
| `LESTRADE_UNIVERSE_TRADING_DATE` | `universe_snapshot` (cross-check), incremental, entrant backfill, enrich | ISO session date **`YYYY-MM-DD`**. |
| `LESTRADE_BARCHART_DIR` | `universe_snapshot` | Absolute path to folder containing exactly one highs + one lows CSV. Default in Docker: `/opt/airflow/lestrade/barchart_report_today`; override if you use e.g. `barchart_report_files/today`. |
| `LESTRADE_BARCHART_ARCHIVE_DIR` | `universe_snapshot` | Archive destination after successful snapshot. |
| `LESTRADE_ENTRY_BACKFILL_DAYS` | `edgar_backfill_on_entry` | Calendar-day lookback window (default 365 in code; often reduced to 90). |
| `LESTRADE_ENTRY_BACKFILL_TIMEOUT_HOURS` | `edgar_backfill_on_entry` | Airflow **`execution_timeout`** (default 6). |
| `LESTRADE_ENRICH_MARKET_TIMEOUT_HOURS` | `enrich_universe_market_context` | Airflow task cap (default 8). |
| `LESTRADE_ISSUER_SEC_SCOPE` | `enrich_issuer_sec_profile` | `universe` (default; needs trading date) or `security_master` (limited backfill). |
| `LESTRADE_ISSUER_SEC_SECURITY_MASTER_LIMIT` | `enrich_issuer_sec_profile` | Max CIKs when scope is `security_master` (default 500). |
| `LESTRADE_ENRICH_ISSUER_SEC_TIMEOUT_HOURS` | `enrich_issuer_sec_profile` | Airflow task cap (default 4). |
| `LESTRADE_ENRICH_PROFILE_TIMEOUT_HOURS` | `enrich_universe_company_profile` | Airflow task cap (default 4). |
| `LESTRADE_YFINANCE_SLEEP_S` | `enrich_universe_company_profile` | Sleep between Yahoo calls (default 0.35). |
| `LESTRADE_YFINANCE_TIMEOUT_S` | `enrich_universe_company_profile` | Per-ticker timeout hint for yfinance (default 25). |
| `LESTRADE_STOOQ_APIKEY` | `enrich_universe_market_context` | Stooq CSV API key when required. |
| `LESTRADE_CHROMA_PERSIST_DIR` | `embed_form4_transactions` | Chroma persist path (Compose: `/opt/airflow/chroma` on volume `lestrade-chroma`). |
| `LESTRADE_EMBED_TRADING_DATES` | `embed_form4_transactions` | Comma-separated ISO dates for multi-day backfill; overrides single `LESTRADE_UNIVERSE_TRADING_DATE` when set. |
| `LESTRADE_EMBED_BATCH_SIZE` | `embed_form4_transactions` | Embed upsert batch size (default 32). |
| `LESTRADE_EMBED_TIMEOUT_HOURS` | `embed_form4_transactions` | Per-task Airflow cap (default 4). |
| `VOYAGE_API_KEY` | `embed_form4_transactions` (Voyage task) | Voyage Finance embed API key. |
| `LESTRADE_RAW_STORAGE` | Ingest DAGs | `bytea` (default) or `s3` (+ bucket). |
| `AIRFLOW_CONN_LESTRADE_RDS` | All Postgres DAGs | Connection URI for `lestrade_rds`. |

After changing `.env`, recreate or restart Airflow workers so variables are picked up.

---

## 4. SEC rate limiting (contract alignment)

**DATA_CONTRACT.md §9.4** recommends ~**120–150 ms** minimum spacing per process toward a **≤10 req/s** aggregate cap.

**This repo:** `EdgarClient` defaults to **`min_interval_s=0.12`** (~8.3 req/s theoretical max for a single process). **Airflow DAGs** pass **`min_interval_s=0.5`** (≈2 req/s per worker) for extra headroom against 429s and WAF behavior.

**Pools:** Tasks that touch EDGAR use pool **`edgar_http`**; tune slot count so total concurrent workers × your effective RPS stays within SEC guidance.

---

## 5. Monitoring (SQL)

Run against the **Lestrade** database (service role or ingest role). Adjust time windows as needed.

### 5.1 Ingestion health (last 24 hours)

```sql
-- Parsed filings and transaction rows
select
  count(*) filter (where parsed_at >= now() - interval '24 hours') as filings_parsed_24h,
  count(*) as filings_total
from filing;

select
  count(*) filter (where exists (
    select 1 from filing f
    where f.accession_number = t.accession_number
      and f.parsed_at >= now() - interval '24 hours'
  )) as txn_rows_for_recently_parsed_filings_24h
from form4_transaction t;
```

### 5.2 Errors by stage

```sql
select stage, error_type, count(*) as n
from ingestion_errors
where created_at >= now() - interval '24 hours'
group by 1, 2
order by n desc;
```

### 5.3 Universe snapshot for a session

```sql
-- Replace date
select universe_group, count(*) as n, count(*) filter (where cik is not null) as with_cik
from universe_snapshot
where trading_date = date '2026-05-11'
group by 1;
```

### 5.4 Raw fetch vs parse gaps

```sql
-- Accessions with raw but no successful parse (parse_error set or missing filing row pattern)
select count(*) as raw_without_clean_parse
from filing_raw r
left join filing f on f.accession_number = r.accession_number
where f.accession_number is null
   or f.parse_error is not null;
```

### 5.5 Long-running task progress (entrant backfill)

While **`edgar_backfill_on_entry`** runs:

```sql
select count(*) as filings_parsed_last_15m
from filing
where parsed_at >= now() - interval '15 minutes';
```

### 5.6 Supabase / pooler disconnects

If you see **`SSL SYSCALL error: EOF detected`** or **`connection already closed`**, the DB session dropped mid-task (idle timeout, pool mode, network sleep). **Mitigation:** use a connection string suited to long tasks (direct DB vs pooler session mode per Supabase docs), keep the host awake, enable **Airflow retries** on the backfill task, and rerun; idempotent upserts make reruns safe.

---

## 6. Validation checklist (DATA_CONTRACT alignment)

Use this after a non-trivial ingest or parser change.

### 6.1 Idempotency and keys

- [ ] **No duplicate accessions:** `select accession_number, count(*) from filing group by 1 having count(*) > 1` → empty.
- [ ] **No duplicate transaction keys:** `select accession_number, transaction_index, count(*) from form4_transaction group by 1,2 having count(*) > 1` → empty.
- [ ] **Re-run safety:** trigger **`edgar_daily_incremental`** twice for the same stamp; row counts for `filing` / `form4_transaction` should not double for the same accession.

### 6.2 `transaction_index` (§6)

Implementation walks **`nonDerivativeTransaction`** nodes in document order (indices `0..k-1`), then **`derivativeTransaction`** nodes (`k..N-1`). Spot-check one known filing: indices contiguous `0..N-1` with no gaps for successfully parsed rows.

```sql
select accession_number, min(transaction_index), max(transaction_index), count(*) as n
from form4_transaction
where accession_number = 'YOUR-ACCESSION-HERE'
group by 1;
-- Expect max - min + 1 = n when indices are dense
```

### 6.3 `total_value` (§5)

- Non-derivative: `shares * price_per_share` when both present **and** no amount footnotes; otherwise NULL.
- Derivatives: **`total_value`** is NULL unless extended rules are added (`_derivative_total_value()` in code).

```sql
select accession_number, transaction_index, shares, price_per_share, total_value
from form4_transaction
where accession_number = 'YOUR-ACCESSION-HERE'
order by transaction_index
limit 50;
```

### 6.4 Form 4 / 4/A (§3)

- Each accession is its own row; **`document_type`** `4` vs **`4/A`**, **`is_amendment`** on `filing`.
- No merge of 4/A into the original 4 accession in Phase 1.

```sql
select document_type, is_amendment, count(*) from filing group by 1, 2 order by 1, 2;
```

### 6.5 Covered universe (§4.4)

- **`universe_snapshot`** rows exist for the stamped **`trading_date`** before incremental / entrant DAGs.
- Optional: compare distinct CIKs in snapshot to ingest activity for that window.

### 6.6 Sample golden filings (recommended)

Maintain **3–5 known accessions** (mix of simple Form 4, 4/A, derivative-heavy, footnote-heavy). After parser changes:

1. Re-ingest (or `force_reparse` if exposed on backfill DAG params).
2. Compare `form4_transaction` row counts and key fields to saved expectations (spreadsheet or fixture).

---

## 7. Incident response (short)

| Symptom | Likely cause | Action |
|---------|----------------|--------|
| Task **`AirflowTaskTimeout`** | `execution_timeout` too low for volume | Raise `LESTRADE_ENTRY_BACKFILL_TIMEOUT_HOURS` / `LESTRADE_ENRICH_MARKET_TIMEOUT_HOURS` or reduce lookback / universe size. |
| SEC **429 / 403** | Rate or blocking | Lower concurrency; increase `min_interval_s`; verify User-Agent. |
| **`SSL SYSCALL` / connection closed** | DB pool / sleep / long transaction | Retry task; tune connection; avoid laptop sleep. |
| **`ingestion_errors` spike** | Parse/fetch regression | Group by `stage` / `error_type`; reproduce on one accession. |

---

## 8. Phase 3 — embeddings (validation)

1. Apply migration **`008_embedding_index_state.sql`** on Supabase.
2. Ensure Docker Compose includes **`[embed]`** and volume **`lestrade-chroma`**; set **`LESTRADE_CHROMA_PERSIST_DIR=/opt/airflow/chroma`** and **`VOYAGE_API_KEY`** in `.env`.
3. After ingest for the session, trigger **`embed_form4_transactions`** with the same **`LESTRADE_UNIVERSE_TRADING_DATE`** (or **`LESTRADE_EMBED_TRADING_DATES`** for a week).
4. Confirm Postgres bookkeeping:

```sql
SELECT model_id, status, count(*)
FROM embedding_index_state
GROUP BY 1, 2
ORDER BY 1, 2;
```

5. Optional smoke retrieval (host or worker shell with `[embed]`):

```bash
export LESTRADE_CHROMA_PERSIST_DIR=/opt/airflow/chroma
python scripts/eval_retrieval.py
```

Architecture and metadata rules: **`docs/PHASE3_ARCHITECTURE.md`**.

---

## 9. Related documents

- **[DATA_CONTRACT.md](./DATA_CONTRACT.md)** — Grain, keys, 4/A policy, `total_value`, SEC access.
- **[PHASES.md](./PHASES.md)** — Phase 1 scope vs later phases.
- **[db/README.md](../db/README.md)** — Migration order.
- **[docker/airflow/README.md](../docker/airflow/README.md)** — Local Airflow bring-up.
