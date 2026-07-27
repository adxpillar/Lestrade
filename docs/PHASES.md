# SEC Form 4 RAG — Phase Plan

Step-by-step breakdown aligned with **ARCHITECTURE.md**. Week labels are targets; adjust based on team size and scope (full historical backfill extends timelines).

---

## Phase 1 — Data Ingestion Pipeline (Week 1)

**Objective:** Reliably discover, download, parse, and persist Form 4 filings with idempotency and observability.

### Steps

1. **Data contract** — see **[DATA_CONTRACT.md](./DATA_CONTRACT.md)** (normative).
   - Grain: transaction-level rows + filing-level parent; keys and types defined there.
   - XML mapping, `total_value` rules, stable `transaction_index`, Form **4/A** handling.
   - Resilient **EDGAR** access + **MWAA** + **RDS** patterns documented in the same file.

2. **EDGAR access**
   - Register **User-Agent** (app name + contact email) per SEC guidance.
   - Implement **discovery**: by **CIK** (submissions JSON) for a **covered universe** of issuers.
   - Implement **fetch** with retries, backoff, respect **~10 requests/second**, and record HTTP status.

3. **Postgres schema**
   - Apply **`db/migrations/`** in order (`db/README.md`); core objects: `filing_raw`, `filing`, `form4_transaction`, `ingestion_errors`, plus `security_master` / `universe_snapshot` for the covered universe.
   - **Unique** `(accession_number)` and `(accession_number, transaction_index)` (or equivalent per `DATA_CONTRACT.md`).

4. **Python pipeline**
   - Modules: discover → fetch → parse → load.
   - **Parse** defensively (schema drift, bad XML); log failures to `ingestion_errors` or similar.
   - **Dedup**: skip fetch if accession + hash already ingested; upsert on conflict for replays.

5. **Airflow**
   - DAG: **universe snapshot** (Barchart highs/lows CSV pair, date-stamped filenames; resolved to CIKs; persisted in Postgres).
   - DAG: **daily incremental ingest** for the stamped `LESTRADE_UNIVERSE_TRADING_DATE` (CIK-scoped; aligns with `universe_snapshot`, not `max(trading_date)`).
   - DAG: **entrant backfill** — when a new ticker enters the universe, backfill for the last year (or configured range).
   - DAG: **parameterized backfill** (`start_date`, `end_date`, optional CIK list) remains for manual replays / targeted recovery.
   - Cap **parallelism** to stay within SEC limits; use pools if needed.

6. **Quality gates**
   - Sample validation against known filings; monitor counts and error rates per run.

**Deliverables:** Postgres with raw + parsed data; Airflow DAGs (`universe_snapshot`, `edgar_daily_incremental`, `edgar_backfill_on_entry`, `edgar_backfill`); documented idempotency keys; ingestion runbook (User-Agent, limits).

**Hosting notes:** Postgres on **RDS**, **Neon**, or comparable; Airflow on **MWAA** or **Docker/EC2** for early demos.

---

## Phase 2 — Enrichment (Week 1–2)

**In this repository (partial):** daily price cache + per-session market context (**`security_daily_prices`**, **`universe_market_context`**, DAG `enrich_universe_market_context`); Yahoo session issuer row (**`universe_company_profile`**, `enrich_universe_company_profile`); **SEC** canonical issuer row (**`issuer_sec_profile`**, `enrich_issuer_sec_profile`, submissions JSON). See **[PHASE2_DATA.md](./PHASE2_DATA.md)**. Still open: filing-level enrichment + `feature_version`, historical “SIC as-of filing date”, Yahoo fallback.

**Objective:** Add market and issuer context without blocking core EDGAR ingest.

### Steps

1. **Source-of-truth table**
   - List each field and **provider**: e.g., CIK↔ticker from **SEC company tickers**; sector/industry/market cap from **yfinance** and/or **SIC** mapping (SEC does not put market cap in the basic tickers JSON).

2. **Price cache schema**
   - `security_daily_prices`: ticker, date, OHLCV, `source`, `ingested_at`.
   - Build windows from **cached daily rows**, not one ad-hoc call per filing.

3. **Enrichment features**
   - **V1 alignment:** compute features per **(universe `trading_date`, ticker)** so the landing page “highs/lows for the session” has market context immediately.
   - For each ticker (anchor date = `universe_snapshot.trading_date`): compute **1d/5d/20d returns** and **20d volatility** (or similar).
   - `company_profile` / issuer columns: sector, market cap, SIC, `as_of`, `source`.

4. **Symbol resolution**
   - CIK → primary listing symbol **as of filing context**; handle multiple share classes and ticker changes.

5. **Pipeline & idempotency**
   - **Separate** Airflow DAG or task group after parse succeeds.
   - Upsert by `(filing_id or accession, feature_version)`; nullable columns + `enrichment_status` for failures.

6. **Throttling and resilience**
   - Low concurrency to Yahoo; backoff; optional fallback provider for production.

**Deliverables:** Enrichment tables; cached prices; rerun-safe jobs; short data dictionary (field → source).

---

## Phase 3 — Embedding + Vector Store (Week 2)

**Status:** Implemented — package `src/lestrade_embed/`, DAG `embed_form4_transactions`, migration `008`, Chroma volume in Compose.

**Architecture (locked):** [PHASE3_ARCHITECTURE.md](./PHASE3_ARCHITECTURE.md) — MiniLM + Voyage Finance 2, Chroma on Docker volume, `embedding_index_state`, universe-scoped scope, Option 3 for `universe_group` metadata.

**Objective:** Create searchable, filterable vectors from structured transaction narratives.

### Steps

1. **Text template**
   - Per transaction chunk, e.g.: *On [date], [person], [title] at [company] ([ticker]) [bought/sold] [N] shares at $[price]/share, totaling $[value]. Ownership type: [direct/indirect].*
   - Handle **missing** price/value, **aggregates**, **derivatives** with explicit phrases (no fake numbers).

2. **Optional enrichment line**
   - Append one **deterministic** sentence from Phase 2 (e.g., market context) if desired for semantic retrieval.

3. **Embedding model**
   - Choose **one** model for v1 (`text-embedding-3-small` *or* `all-MiniLM-L6-v2`); record **dimension** in config. Do not mix models in one index.

4. **Vector IDs and upsert**
   - Stable ID: e.g. `accession` + `transaction_index` (hash or concatenation per DB limits).
   - **Upsert** on re-ingest; align with Form 4/A replace policy.

5. **Metadata**
   - Filterable: ticker, transaction date, filing date, person, transaction type (P/S), CIK, accession (for citation and dedup).
   - Keep payload within provider limits; long text lives in the embedding input / document store as required.

6. **Stores**
   - **Dev:** Chroma (persistent path, single writer discipline).
   - **Demo/prod:** Pinecone (or Qdrant / pgvector) with namespaces or index per environment.

7. **Abstraction**
   - Thin interface: `upsert`, `delete_by_accession`, `query(filter, query_vector)` to limit vendor lock-in.

8. **Orchestration**
   - Trigger on new/updated parsed rows (and enrichment if text depends on it); batch upserts.

**Deliverables:** Populated index; documented model + dimension; eval notebook or script with a small fixed query set (optional but recommended).

---

## Phase 4 — RAG Query Layer (Week 2–3)

**Objective:** Natural-language Q&A with filters, follow-ups, and grounded citations.

### Steps

1. **Retrieval chain**
   - LangChain (LCEL) or equivalent: **apply metadata filters** → **vector search** → assemble context.

2. **Hybrid router**
   - Route **filter/numeric/exhaustive** questions to **SQL** (Postgres); **fuzzy** questions to **RAG**.
   - Optionally: SQL returns rows, LLM summarizes with strict “only from table” instructions.

3. **Session state (critical for follow-ups)**
   - Server-side **structured state**: last ticker, date range, persons, transaction type, limits.
   - Merge user follow-up (“only buys”) into filters and **re-query**; use short chat history for **entity resolution** (“they” → last insider).

4. **Optional query rewrite**
   - Small step: user message + state → updated filters + search query string (LLM or rules).

5. **System prompt**
   - Require **citations**: accession (and SEC URL pattern), **filing vs transaction date** (define terms), amounts, buy/sell.
   - Instruct: no fabrication; if retrieval empty, say so and suggest broadening filters.

6. **Grounding enforcement**
   - Prefer answers **only** from retrieved chunks / tool results; optional structured JSON claims with `source_ids` before prose.

7. **Observability**
   - Log filters, retrieval IDs, model, latency per request.

**Deliverables:** Working chat with citations; follow-up behavior demo; documented router rules; env-based model/API configuration.

---

## Phase 5 — UI + Deployment (Week 3)

**Objective:** Clean, portfolio-ready interface and a deployable stack.

### Steps

1. **Streamlit layout**
   - Sidebar: ticker, date range, person, buy/sell, optional advanced filters.
   - Main: chat; **sources** panel (table or expanders: accession, dates, amounts, link to EDGAR).

2. **Integration**
   - Call Phase 4 layer (in-process Python **or** HTTP API to FastAPI if separating early).

3. **Caching**
   - Use `st.cache_resource` / `st.cache_data` (or backend caching) to avoid duplicate embed/API calls on reruns.

4. **Secrets and config**
   - Environment variables or host secret store; no keys in repo.

5. **Deployment**
   - Choose target: **Streamlit Community Cloud**, **ECS/Fargate**, **Render**, **Fly.io**, etc.
   - Ensure DB and vector DB are **network-reachable** from the app (not localhost in cloud).

6. **Hardening (minimum for public URL)**
   - Simple auth or allowlist; rate limiting if exposed to internet.

7. **README**
   - Architecture pointer, how to run locally, compliance disclaimer (not investment advice; EDGAR attribution).

**Deliverables:** Public or demo URL (optional); reproducible local run; screenshots for portfolio.

---

## Cross-phase dependencies

| Phase | Depends on |
|-------|------------|
| 2 | 1 (parsed filings, CIK, dates) |
| 3 | 1; 2 optional for enriched text |
| 4 | 1; 3; 2 optional for SQL/enrichment |
| 5 | 4 (and connectivity to DB + vector store) |

**Parallelization:** Phase 2 jobs can run alongside Phase 1 once first filings land. Phase 3 can start on a **subset** of data while Phase 2 backfills. Phase 4 can prototype single-turn RAG before full session state.

---

## Suggested evaluation set (cross-cutting)

Maintain ~10 benchmark questions: **5 structured** (SQL-leaning) and **5 fuzzy** (RAG-leaning). Score: correct accessions, correct buy/sell and amounts, no hallucinated filings. Re-run after major schema or prompt changes.
