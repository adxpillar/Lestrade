# Phase 2 — enrichment data dictionary

Short **field → source** reference for tables populated after Phase 1 ingest. Normative ingest rules remain in **[DATA_CONTRACT.md](./DATA_CONTRACT.md)**.

---

## `security_daily_prices`

| Column | Source |
|--------|--------|
| `ticker`, `price_date` | Universe / Stooq symbol (`*.us`); calendar date from Stooq daily CSV |
| `open`, `high`, `low`, `close` | Stooq daily CSV; values outside Postgres `NUMERIC(18,6)` range are dropped or nulled |
| `volume` | Stooq CSV; must fit Postgres `BIGINT` |
| `source` | Default `stooq` |
| `ingested_at` | Pipeline write time (UTC) |

---

## `universe_market_context`

| Column | Source |
|--------|--------|
| `trading_date`, `universe_group`, `ticker` | **`universe_snapshot`** for the stamped session |
| `cik` | **`universe_snapshot.cik`** for that `(trading_date, ticker)` |
| `close`, `volume` | Last available bar on or before `trading_date` from **`security_daily_prices`** |
| `ret_1d`, `ret_5d`, `ret_20d` | Close-to-close returns from cached daily closes |
| `vol_20d` | Sample stddev of daily returns over up to 20 prior bars (not annualized) |
| `source` | Default `stooq` |

---

## `universe_company_profile`

| Column | Source |
|--------|--------|
| `trading_date`, `ticker` | Distinct tickers from **`universe_snapshot`** for the session |
| `cik` | Representative `max(cik)` from snapshot rows for that ticker (usually one CIK) |
| `sector`, `industry`, `market_cap`, `quote_type`, `long_name`, `currency` | **Yahoo Finance** via **`yfinance`** `Ticker.info` (best-effort; keys vary by listing) |
| `sic_code` | **`yfinance`** `industryKey` or `industryDisp` when present — **not** SEC SIC; use **`issuer_sec_profile.sic_code`** for official SIC when populated. |
| `enrichment_status` | `ok` \| `no_data` \| `error` |
| `error_message` | Set when status is `error` |
| `source` | Default `yfinance` |
| `fetched_at`, `updated_at` | Upsert time (UTC) |

**DAG:** `enrich_universe_company_profile` (install package extra **`[enrich]`**; Docker Compose uses **`[ingest,enrich]`**).

**Throttling:** `LESTRADE_YFINANCE_SLEEP_S` between tickers (default **0.35** s). Respect Yahoo terms of use in production.

---

## `issuer_sec_profile` (SEC canonical)

| Column | Source |
|--------|--------|
| `cik` | **Primary key** — issuer (zero-padded 10 digits) |
| `entity_name`, `entity_type` | SEC company submissions JSON top-level (`name`, `entityType`) |
| `sic_code`, `sic_description` | SEC **`sic`**, **`sicDescription`** (Standard Industrial Classification) |
| `state_of_incorporation`, `fiscal_year_end` | SEC submissions when present |
| `tickers`, `exchanges` | SEC parallel arrays **`tickers`** / **`exchanges`** when present |
| `fetched_at`, `updated_at` | Last successful or failed upsert attempt (UTC) |
| `source` | Default **`sec_submissions`** |
| `enrichment_status` | `ok` \| `error` |
| `error_message` | Set when fetch or parse fails for that CIK |

**DAG:** `enrich_issuer_sec_profile` — calls **`data.sec.gov/submissions/CIK##########.json`** (same contract as ingest discovery). Use pool **`edgar_http`** and **`min_interval_s=0.5`** per worker.

**Scope:** `LESTRADE_ISSUER_SEC_SCOPE` = `universe` (default; needs **`LESTRADE_UNIVERSE_TRADING_DATE`**) or `security_master` (capped by **`LESTRADE_ISSUER_SEC_SECURITY_MASTER_LIMIT`**, default 500).

### Temporal binding (v1)

- **`issuer_sec_profile`** reflects the **latest** company submissions JSON at **`fetched_at`**. It is **not** automatically “SIC as of `filing.filing_date`.”
- For UI and coarse filters, **join** `filing.issuer_cik` → `issuer_sec_profile.cik` for **current** SEC-reported issuer metadata.
- For strict “as-of filing date” lineage (RAG citations, audits), plan a **filing-scoped** enrichment row or historical snapshot strategy later; see **[PHASES.md](./PHASES.md)** Phase 2 filing-level steps.

---

## Related

- **[PHASES.md](./PHASES.md)** — full Phase 2 objectives (symbol resolution, filing-level enrichment, fallbacks).
- **[PHASE1_RUNBOOK.md](./PHASE1_RUNBOOK.md)** — env vars and DAG ordering with ingest.
