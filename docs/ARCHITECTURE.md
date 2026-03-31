# SEC Form 4 Insider Trading RAG — Architecture

This document describes the end-to-end system for ingesting 500K+ SEC Form 4 filings, enriching them, indexing them for semantic search, and answering natural-language questions with grounded citations.

---

## 1. Goals and scope

- **Ingest** Form 4 XML from SEC EDGAR with idempotent, schedulable pipelines.
- **Parse** structured fields: filer, issuer, transactions (dates, P/S, shares, price, ownership type, derivatives where applicable).
- **Enrich** with market and issuer context (prices, sector, market cap) for analytics and LLM grounding.
- **Index** transaction-level text for semantic retrieval with strong metadata for filtering.
- **Query** via a hybrid layer: **SQL** for precise/numeric questions, **RAG** for fuzzy or narrative questions, with **citations** to filings.
- **Present** a portfolio-friendly UI (Streamlit) with filters and conversational follow-ups.

**Design principle:** Postgres holds the **source of truth**; the vector store is a **search index**. Many user questions are answered best with **structured queries**; RAG complements that for language flexibility and summarization.

---

## 2. High-level diagram

```
                    ┌─────────────────┐
                    │   SEC EDGAR     │
                    │ (submissions,   │
                    │  indices, XML)  │
                    └────────┬────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────┐
│ Phase 1 — Ingestion (Python + Airflow)                        │
│  Discover → Fetch XML → Parse → Load Postgres (raw + parsed)  │
└────────────────────────────┬─────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────┐
│ Phase 2 — Enrichment (Airflow, async from core ingest)        │
│  Price cache (yfinance) ±30d around filing date               │
│  Issuer metadata: CIK↔ticker (SEC), sector/SIC/cap (defined   │
│  sources: yfinance / SIC mapping / optional paid API)          │
└────────────────────────────┬─────────────────────────────────┘
                             │
              ┌──────────────┴──────────────┐
              ▼                             ▼
┌─────────────────────────┐     ┌─────────────────────────────┐
│ Postgres                │     │ Phase 3 — Embedding          │
│  filings, transactions, │     │  Template text → embed       │
│  prices, profiles,      │────▶│  Upsert Chroma / Pinecone    │
│  enrichment features    │     │  + metadata for filters      │
└────────────┬────────────┘     └──────────────┬──────────────┘
             │                                │
             └────────────┬───────────────────┘
                          ▼
┌──────────────────────────────────────────────────────────────┐
│ Phase 4 — RAG / Query Layer                                    │
│  Router: SQL tool vs semantic retrieval                        │
│  Metadata filter → vector search → LLM (GPT-4o / Claude)      │
│  Session state (filters + entities) for follow-ups             │
│  System prompt: cite accession, dates, amounts, buy/sell     │
└────────────────────────────┬─────────────────────────────────┘
                             ▼
┌──────────────────────────────────────────────────────────────┐
│ Phase 5 — UI + Deployment                                      │
│  Streamlit: filters + chat + sources                           │
│  Hosted: Streamlit Cloud / ECS / Render + managed Postgres     │
└──────────────────────────────────────────────────────────────┘
```

---

## 3. Data flow and grain

| Layer | Grain | Primary key / idempotency |
|-------|--------|---------------------------|
| Raw filing | One row per accession (or document) | `accession_number`, `content_sha256` |
| Parsed filing | One row per filing | `accession_number` |
| Transactions | One row per non-derivative / derivative line (policy TBD) | `accession_number` + `transaction_index` |
| Daily prices | One row per symbol per trading day | `(symbol, date)` or `(cik, date)` per policy |
| Vectors | One vector per embedded chunk (default: per transaction line) | Stable ID derived from accession + transaction index |

**Amendments (Form 4/A):** Policy must state whether amended filings **replace** prior parsed rows and vectors (recommended: upsert same IDs).

---

## 4. External systems and constraints

| System | Role | Constraints |
|--------|------|-------------|
| SEC EDGAR | Source of Form 4 XML | Rate limits (~10 req/s), required descriptive `User-Agent` with contact |
| Postgres | System of record | Backups, migrations, indexes for filter-heavy queries |
| yfinance (or successor) | OHLCV + some issuer stats | Unofficial; rate limits; cache aggressively |
| OpenAI (or local model) | Embeddings + chat | Cost, latency, data residency |
| Chroma / Pinecone | Vector index | Dimension tied to embedding model; metadata size limits (Pinecone) |
| Airflow | Orchestration | MWAA / self-hosted / Composer (if GCP) |

---

## 5. Hybrid query model (Phase 4)

- **Structured path:** Questions with clear filters and numeric thresholds (e.g., “&gt; $1M”, date ranges, ticker) → **SQL** against Postgres for completeness and accuracy; LLM may **summarize** the result set.
- **Semantic path:** Vague or narrative questions → **metadata filter** (ticker, date, person, P/S) then **vector search**; LLM answers **only** from retrieved chunks (plus optional structured tool results).
- **Session state:** Follow-ups (“only the buys”) update **structured filters** and re-run retrieval, not only chat memory.

---

## 6. Security and operations

- Secrets (DB, API keys) in environment or cloud secret manager; never committed.
- Public demos: protect UI (password, allowlist) to avoid API abuse.
- Logging: discovery counts, parse failures, enrichment status, retrieval filters, and latency for debugging.
- Compliance posture: EDGAR attribution; no investment advice; define data retention and terms of use for any public deployment.

---

## 7. Technology choices (summary)

| Component | Recommended default | Alternatives |
|-----------|---------------------|--------------|
| Database | AWS RDS PostgreSQL | Neon, Supabase, Cloud SQL |
| Orchestration | AWS MWAA | Airflow on EC2/Docker, Prefect, Dagster |
| Embeddings | `text-embedding-3-small` | `all-MiniLM-L6-v2` (local; separate index dimension) |
| Vector DB (dev) | Chroma | — |
| Vector DB (demo/prod) | Pinecone | Qdrant, pgvector |
| RAG framework | LangChain (LCEL) | LlamaIndex, thin custom pipeline |
| LLM | GPT-4o or Claude API | — |
| UI | Streamlit | FastAPI + frontend later |

---

## 8. Related documents

- **[PHASES.md](./PHASES.md)** — Step-by-step work per phase, deliverables, and sequencing.
- **[DATA_CONTRACT.md](./DATA_CONTRACT.md)** — Phase 1 grain, Postgres tables, field rules, and resilient EDGAR + MWAA + RDS access.
