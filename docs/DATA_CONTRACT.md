# Phase 1 — Data contract and EDGAR access strategy

This document is the **normative data contract** for ingestion (Phase 1). It defines grain, identifiers, logical fields, XML mapping expectations, amendment policy, Postgres-oriented types, and a **resilient EDGAR access strategy** compatible with **AWS RDS** and **Amazon MWAA**.

**Schema definitions:** versioned SQL lives under **`db/migrations/`** (see **`db/README.md`**); this document summarizes behavior and logical fields—treat migrations as authoritative for DDL.

**Authoritative XML rules:** [EDGAR Ownership XML Technical Specification](https://www.sec.gov/info/edgar/ownershipxmltechspec-v2.htm) (v2; v4 draft exists—implement parsers to tolerate both common shapes in the wild).

---

## 1. Grain and scope

| Entity | Grain | Description |
|--------|--------|-------------|
| **Filing** | One row per SEC **accession** | One Form 4 or 4/A submission package; holds issuer, period, schema version hints, and links to raw artifact. |
| **Transaction** | One row per **non-derivative** or **derivative** transaction node | Each repeating transaction block in the XML produces one row, in **document order** (see §6). |

**In scope for Phase 1 parse:** `documentType` **4** and **4/A** only. Other ownership forms (3, 5, etc.) are out of scope unless explicitly added later.

**Primary human-facing unit:** insider **transaction** lines; filings are the **parent** and deduplication anchor.

---

## 2. Identity and deduplication

| Concept | Rule |
|---------|------|
| **accession_number** | SEC accession string, e.g. `0000320193-25-000042`. **Natural key** for a filing. Immutable for that submission. |
| **cik** | Issuer **Central Index Key**, stored as **10-character zero-padded string** (e.g. `0000320193`) for consistency with URLs and JSON APIs. |
| **accession_nodash** | Accession with hyphens removed; used in EDGAR directory paths. |
| **transaction_index** | `0..N-1` **stable ordering** within the filing XML (§6). Unique per accession. |
| **content_sha256** | SHA-256 of the **primary Form 4 XML** bytes as fetched. Detects silent re-posts or corruption vs prior fetch. |
| **Idempotent load** | Upsert filing by `accession_number`; upsert transactions by `(accession_number, transaction_index)`. Re-runs must not duplicate rows. |

---

## 3. Form 4/A (amendments) policy

**Policy: upsert by accession; each filing is its own accession.**

- Each EDGAR submission has a **unique** `accession_number`. A Form **4/A** is a **new** accession, not an update to the original Form 4’s accession row in EDGAR.
- **“Replace” in Phase 1 means:** when you **re-ingest the same accession** (DAG replay, parser fix), **delete or upsert** rows keyed by that accession so you do not duplicate transactions. It does **not** mean merging 4/A into the original 4’s primary key.
- **Clarification:** A Form 4/A has its **own** accession. “Replace” for amendments at the **product** level means:
  - **Do not** merge 4/A rows into the original Form 4 accession in Phase 1 unless you build an explicit **amendment chain** (out of scope for minimal Phase 1).
  - **Do** parse each accession independently; use **`is_amendment`**, **`document_type`**, and SEC-reported links if present in XML/metadata for downstream “latest truth” logic.
- **Practical minimal rule:** Ingest **every** accession as its own filing + transactions. Optional column **`supersedes_accession`** or **`remarks`** may be populated later from footnotes—nullable in v1.

If product requirements later demand **one logical event** across 4 → 4/A, add a **resolution layer** (Phase 2+), not ambiguous ingestion keys.

---

## 4. Logical field catalog

### 4.1 `filing_raw` (immutable artifact metadata)

Stores **provenance** and **bytes location**; parsing reads from here or S3.

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `accession_number` | `TEXT` | PK | SEC accession. |
| `cik` | `CHAR(10)` | Y | Issuer CIK (padded). |
| `filing_date` | `DATE` | Y | Filing date as reported by SEC index or submissions (ET calendar date). |
| `accepted_at` | `TIMESTAMPTZ` | N | SEC acceptance datetime if available from index/API. |
| `accepted_at_atl_timestamp` | `TIMESTAMP` | N | Generated ET (America/New_York) local timestamp derived from `accepted_at`. |
| `primary_document` | `TEXT` | Y | Filename of primary XML, e.g. `xslF345X05/wf-form4_*.xml`. |
| `raw_xml_s3_uri` | `TEXT` | N | `s3://bucket/...` if raw bytes live in S3 (recommended at scale). |
| `raw_xml` | `BYTEA` | N | Inline bytes only if small-dev mode; prefer S3 + NULL here in production. |
| `content_sha256` | `CHAR(64)` | Y | Hex SHA-256 of raw XML. |
| `fetched_at` | `TIMESTAMPTZ` | Y | When bytes were retrieved. |
| `fetched_at_atl_timestamp` | `TIMESTAMP` | Y | Generated ET (America/New_York) local timestamp derived from `fetched_at`. |
| `http_status` | `SMALLINT` | Y | Last HTTP status from EDGAR fetch. |
| `fetch_error` | `TEXT` | N | Last error message if any. |

**Dedup:** Before fetch, if `accession_number` exists with same `content_sha256`, skip re-download (optional re-fetch policy for staleness can be config).

---

### 4.2 `filing` (parsed header, one per accession)

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `accession_number` | `TEXT` | PK, FK → `filing_raw` | |
| `document_type` | `TEXT` | Y | `4` or `4/A`. |
| `is_amendment` | `BOOLEAN` | Y | `true` if `4/A`. |
| `schema_version` | `TEXT` | N | If detectable from XML / schema hints. |
| `period_of_report` | `DATE` | N | `periodOfReport` from XML. |
| `issuer_cik` | `CHAR(10)` | Y | From `issuer/issuerCik` (normalized pad). |
| `issuer_name` | `TEXT` | N | `issuerName`. |
| `issuer_ticker` | `TEXT` | N | `issuerTradingSymbol` (may be empty). |
| `insider_cik` | `CHAR(10)` | N | Reporting owner CIK if present. |
| `insider_name` | `TEXT` | N | Reporting owner name. |
| `insider_title` | `TEXT` | N | Officer title / relationship fields flattened (e.g. chief executive officer). |
| `parsed_at` | `TIMESTAMPTZ` | Y | When parse completed. |
| `parsed_at_atl_timestamp` | `TIMESTAMP` | Y | Generated ET (America/New_York) local timestamp derived from `parsed_at`. |
| `parse_error` | `TEXT` | N | Set if filing-level parse failed partial. |

---

### 4.3 `form4_transaction` (parsed lines)

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `accession_number` | `TEXT` | PK part, FK → `filing` | |
| `transaction_index` | `INTEGER` | PK part | Stable order in XML (§6). |
| `transaction_category` | `TEXT` | Y | `non_derivative` \| `derivative`. |
| `security_title` | `TEXT` | N | Security description. |
| `transaction_date` | `DATE` | N | Transaction date for the line. |
| `transaction_code` | `TEXT` | N | e.g. **P** (open market purchase), **S** (sale), **A** (grant), **M** (exercise)—full table in SEC instructions; store raw code. |
| `acquired_disposed_code` | `CHAR(1)` | N | **A** / **D** when present (acquired vs disposed). |
| `shares` | `NUMERIC(28, 8)` | N | Share amount when scalar; NULL if only footnote / range / split. |
| `price_per_share` | `NUMERIC(28, 8)` | N | Price when scalar; NULL if gift, range, etc. |
| `total_value` | `NUMERIC(28, 4)` | N | **Derived** (§5). |
| `direct_indirect` | `TEXT` | N | `D` / `I` or normalized `direct` / `indirect` from `ownershipNature`. |
| `footnote_ids` | `TEXT[]` | N | Optional: footnote id strings attached to amounts (for traceability). |

**Note:** `form4_transaction` has no `*_atl_timestamp` generated columns in the current migrations; migration **`004`** adds ET-derived timestamps only to `filing_raw`, `filing`, `ingestion_errors`, `security_master`, and `universe_snapshot`.

**Indexes (recommended):** `(issuer_cik, transaction_date)`, `(issuer_ticker, transaction_date)` where ticker not null, `(insider_cik, transaction_date)`, `(transaction_code, transaction_date)`.

---

### 4.4 Covered universe (`security_master`, `universe_snapshot`)

These tables scope **who** gets ingested (issuer CIKs derived from a daily Barchart high/low export). DDL is in **`db/migrations/003_universe_snapshot.sql`**.

| Table | Role |
|--------|------|
| **`security_master`** | Cache: normalized **ticker → CIK** (from SEC `company_tickers.json` during `universe_snapshot`). |
| **`universe_snapshot`** | One row per **(trading_date, ticker, universe_group)** with `universe_group` ∈ {`high`, `low`}; holds resolved `cik` when mapping succeeds. |

The **`universe_snapshot`** DAG reads **`MM-DD-YYYY`** from both Barchart filenames and requires they match; optional **`LESTRADE_UNIVERSE_TRADING_DATE`** (ISO) must agree when set. Other DAGs use the env stamp for ingest windows—not `max(trading_date)` in Postgres—so reruns stay tied to the intended session.

---

## 5. `total_value` derivation rules

Compute **once at ingest**; store NULL when not meaningful.

| Condition | `total_value` |
|-----------|----------------|
| `shares` and `price_per_share` both non-NULL | `shares * price_per_share` |
| Either missing, or XML indicates **aggregate** / **range** / **see footnote** without scalars | `NULL` |
| **Derivative** rows where economics are not “shares × price” in the same way | `NULL` unless a documented rule applies later |

Do **not** infer prices from footnote prose in Phase 1.

---

## 6. Stable `transaction_index` ordering

To guarantee **idempotent** upserts across parser versions:

1. Depth-first walk of `nonDerivativeTable`: assign indices `0..k-1` to each `nonDerivativeTransaction` in XML order.
2. Then `derivativeTable`: assign `k..N-1` to each `derivativeTransaction` in XML order.

If a table is absent, skip. If parser skips malformed nodes, log to `ingestion_errors` and **do not** reuse indices for a different node without a **parser_version** bump and re-ingest job.

---

## 7. XML mapping (implementation hint)

Typical paths under `ownershipDocument` (exact names must follow current SEC samples and spec):

| Logical field | Typical XML context |
|---------------|---------------------|
| `document_type` | `documentType` |
| `period_of_report` | `periodOfReport` |
| Issuer | `issuer/*` |
| Insider | `reportingOwner/reportingOwnerId`, `reportingOwnerRelationship` |
| Non-derivative rows | `nonDerivativeTable/nonDerivativeTransaction/*` — `transactionDate`, `transactionCoding/transactionCode`, `transactionAmounts/transactionShares`, `transactionAmounts/transactionPricePerShare`, `ownershipNature/directOrIndirectOwnership` |
| Derivative rows | `derivativeTable/derivativeTransaction/*` (analogous amount/price nodes per spec) |

Footnotes: `footnotes/footnote[@id]`; optional linkage via footnote attributes on child elements.

**Parser requirement:** Defensive `lxml` (or equivalent) with **no silent failure** on unknown tags; collect unknowns in logs for schema drift.

---

## 8. `ingestion_errors` (recommended)

| Column | Type | Description |
|--------|------|-------------|
| `id` | `BIGSERIAL` | |
| `accession_number` | `TEXT` | Nullable if discovery failed pre-accession. |
| `stage` | `TEXT` | `discover` \| `fetch` \| `parse` \| `load` |
| `error_type` | `TEXT` | Short code |
| `message` | `TEXT` | |
| `payload` | `JSONB` | Snippet or context |
| `created_at` | `TIMESTAMPTZ` | |
| `created_at_atl_timestamp` | `TIMESTAMP` | Generated ET (America/New_York) local timestamp derived from `created_at`. |

---

## 9. Resilient EDGAR access strategy (RDS + MWAA)

### 9.1 SEC fair access (non-negotiable)

- Send a descriptive **`User-Agent`** identifying the app and a **contact email** (SEC policy).
- Target **≤ 10 requests per second** to `www.sec.gov` / `data.sec.gov` in aggregate across **all** concurrent tasks.
- Prefer **`data.sec.gov`** for **submissions JSON**; **`www.sec.gov`** for **Archives** bulk files and filing documents as per SEC documentation.

### 9.2 Discovery (how you find Form 4 accessions)

| Mode | Mechanism | Use case |
|------|-----------|----------|
| **By issuer (submissions JSON)** | `GET https://data.sec.gov/submissions/CIK##########.json` | Walk `filings.recent` / `filings.files` for `form` in (`4`, `4/A`). **Primary path** for daily incremental and universe-scoped backfills: CIK list comes from `universe_snapshot` + `LESTRADE_UNIVERSE_TRADING_DATE`, or from parameterized `edgar_backfill` CIK list. |
| **By date range (master index)** | Daily **`master.idx`** under `https://www.sec.gov/Archives/edgar/daily-index/YYYY/QTR/master.idx` | **Optional** broad backfill when Archives endpoints are reachable from your network; not required for the default universe-driven pipeline. |

**Implementation note:** Submissions JSON provides accession, filing date, and `primaryDocument` for each recent filing; the pipeline fetches primary XML (with HTML→XML resolution when needed). Daily **market-wide** index discovery is **not** a prerequisite for production ingest in this repo.

**Do not** spawn unbounded parallel discovery workers against SEC.

### 9.3 Fetch (retrieve XML)

- URL pattern: `https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_nodash}/{primary_document}` with CIK **without leading zeros** in the path segment (SEC convention).
- **Retries:** exponential backoff + jitter on **429**, **5xx**, and transient network errors; cap attempts; log terminal failures to `ingestion_errors`.
- **Integrity:** compute `content_sha256`; optionally store raw XML in **S3** and keep only URI + hash in RDS to keep the database small.

### 9.4 Rate limiting under MWAA

Airflow **Pools** limit concurrent tasks but **do not** enforce requests/second across tasks. Use a **combination** of:

1. **Pool `edgar_http`** with a **low slot count** (e.g. **4–6**) for any task that touches EDGAR.
2. **Shared client** in code using a **token bucket** or **minimum inter-request sleep** (e.g. **120–150 ms** between calls) **per process**. For strict global 10 RPS across multiple workers, prefer **central serialization**:
   - **Phase 1 simple path:** one **batch task** per DAG run that processes a list of accessions **sequentially** with an in-process limiter (predictable, easy to reason about).
   - **Scale path:** offload fetches to a **single-concurrency** worker (e.g. SQS consumer with reserved concurrency = 1) or Step Functions with serial fetch—optional later.

**Recommendation for Week 1:** **Batch sequential fetch** inside a pooled task for each partition (e.g. 50–200 filings per task), with in-process `min_interval` between HTTP calls.

**Lestrade implementation:** `EdgarClient` supports a configurable `min_interval_s` (default **0.12** s in code). **Airflow ingest DAGs** construct the client with **`min_interval_s=0.5`** (~2 requests/s per worker process) for conservative operation against SEC edge behavior. Tasks use pool **`edgar_http`**. Tune pool slots × workers so aggregate traffic remains within SEC guidance.

### 9.5 MWAA operational practices

- Store **`DATABASE_URL`** (or host/user/password) in **AWS Secrets Manager**; MWAA reads via connection or startup script; **no secrets** in DAG code or Git.
- Run RDS in **same VPC** as MWAA (or peered); security groups allow **Postgres 5432** from MWAA workers only.
- **Connections:** use **Airflow Connections** for Postgres; enable **connection pooling** in app code or limit concurrent writers to avoid exhausting RDS `max_connections`.
- **DAG design (universe + filings):**
  - **`universe_snapshot`:** read Barchart highs/lows CSVs for the stamped date → resolve ticker→CIK → upsert `security_master` / `universe_snapshot`.
  - **`edgar_daily_incremental`:** for **`LESTRADE_UNIVERSE_TRADING_DATE`**, ingest **only** CIKs present in that snapshot; submissions-based discovery per issuer (no market-wide daily index).
  - **`edgar_backfill_on_entry`:** compare entrants vs prior snapshot **for the same stamped date context**; backfill new tickers over **`LESTRADE_ENTRY_BACKFILL_DAYS`**.
  - **`edgar_backfill`:** parameterized `start_date`, `end_date`, optional CIK list; manual recovery / targeted history.
  - **`enrich_universe_market_context`:** Phase 2 market context (Stooq, etc.) after universe is stamped; uses **`LESTRADE_UNIVERSE_TRADING_DATE`** and migration **`005`** tables (see **`db/README.md`**).
- **Idempotency:** DAG reruns **safe** due to PK upserts on `accession_number` and `(accession_number, transaction_index)`.

### 9.6 RDS specifics

- Enable **automated backups** and a **Multi-AZ** (or at least snapshot) policy for anything beyond dev.
- Use **`TIMESTAMPTZ`** for all wall-clock fields.
- Consider **pgcrypto** or app-layer SHA256 for `content_sha256`.

---

## 10. Deliverables checklist (Phase 1 data layer)

Operators and implementers should verify the following (see **[PHASE1_RUNBOOK.md](./PHASE1_RUNBOOK.md)** for DAG order, monitoring SQL, and validation steps):

- Apply **`db/migrations`** in order (**`001` → `007`** for full Phase 2 stack in this repo; Phase 1 filing + universe core is **`001`–`003`**; **`004`** adds ET generated columns; **`005`**–**`007`** add Phase 2 market, Yahoo session profile, and SEC issuer profile — see **`db/README.md`**).
- Tables present: `filing_raw`, `filing`, `form4_transaction`, `ingestion_errors`; universe: `security_master`, `universe_snapshot` (optional S3 for raw XML per `LESTRADE_RAW_STORAGE`). Phase 2 (optional for core ingest): **`security_daily_prices`**, **`universe_market_context`**, **`universe_company_profile`**, **`issuer_sec_profile`** (see **`docs/PHASE2_DATA.md`**).
- **transaction_index** and **total_value** rules documented in §5–§6; implementation in `src/lestrade_ingest/form4_parser.py` and `loader.py`.
- EDGAR **User-Agent**, **retries/backoff**, **in-process interval**, and Airflow **pool `edgar_http`** on tasks that call SEC.
- Runbooks: **[PHASE1_RUNBOOK.md](./PHASE1_RUNBOOK.md)** (submissions-based CIK discovery, monitoring, incident notes); optional **master index** backfill remains documented in §9.2 as optional.

---

## 11. Related documents

- [PHASES.md](./PHASES.md) — Phase 1 steps.
- [ARCHITECTURE.md](./ARCHITECTURE.md) — End-to-end system view.
- [PHASE1_RUNBOOK.md](./PHASE1_RUNBOOK.md) — Operations, monitoring SQL, validation checklist.
- [PHASE2_DATA.md](./PHASE2_DATA.md) — Phase 2 enrichment tables (field → source).

---

## 12. Implementation alignment (this repository)

| DATA_CONTRACT topic | Location in code |
|----------------------|------------------|
| §2 PK / upsert idempotency | `src/lestrade_ingest/loader.py` (`ON CONFLICT` on `filing`, `form4_transaction`); raw dedup in `filing_raw` helpers |
| §3 Form 4/A (separate accession) | Parsed per accession; `document_type` / `is_amendment` on `filing` |
| §5–§6 `total_value`, `transaction_index` | `src/lestrade_ingest/form4_parser.py` |
| §7 XML mapping / defensive parse | `form4_parser.py`; failures → `ingestion_errors` via `src/lestrade_ingest/errors.py` (best-effort if DB connection is lost) |
| §8 `ingestion_errors` | `record_ingestion_error` in `errors.py`; stages `discover` \| `fetch` \| `parse` \| `load` |
| §4.4 Universe | `src/lestrade_ingest/universe.py`; DAG `dags/universe_snapshot.py`; migration `003` |
| §9.2 Submissions discovery | `src/lestrade_edgar/client.py`, `discovery.py`; ingest `ingest_cik_submissions_for_date_range` in `pipeline.py` |
| §9.3 Fetch / HTML→XML | `pipeline.py` (`ingest_form4_ref`), `fetch.py`, `discovery.py` |
| §9.4 Rate limit + pool | `EdgarClient` in `src/lestrade_edgar/client.py`; `min_interval_s=0.5` in `edgar_daily_incremental.py`, `edgar_backfill.py`, `edgar_backfill_on_entry.py`; pool `edgar_http` on those DAGs, `enrich_universe_market_context`, and **`enrich_issuer_sec_profile`** |
| §9.5 `enrich_universe_market_context` (Phase 2) | `dags/enrich_universe_market_context.py`; `src/lestrade_enrich/market.py`; migration `005` |
| §9.5 `enrich_universe_company_profile` (Phase 2) | `dags/enrich_universe_company_profile.py`; `src/lestrade_enrich/company_profile.py`; migration `006`; optional dep **`yfinance`** (`[enrich]`) |
| §9.5 `enrich_issuer_sec_profile` (Phase 2) | `dags/enrich_issuer_sec_profile.py`; `src/lestrade_enrich/sec_issuer_profile.py`; `src/lestrade_edgar/submissions_company.py`; migration `007` |
| ET local timestamps (`*_atl_timestamp`) | Migration `004` generated columns on `filing_raw`, `filing`, `ingestion_errors`, `security_master`, `universe_snapshot` (see **`db/README.md`**) |
