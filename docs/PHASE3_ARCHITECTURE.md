# Phase 3 — Embeddings architecture

Normative plan for transaction-level vectors before implementation. Postgres remains **source of truth**; Chroma is a **rebuildable search index**.

Related: [PHASES.md](./PHASES.md) § Phase 3, [ARCHITECTURE.md](./ARCHITECTURE.md) §5 hybrid query, [PHASE2_DATA.md](./PHASE2_DATA.md).

---

## Locked decisions

| Topic | Decision |
|-------|----------|
| Chunk grain | One vector per **`form4_transaction`** row |
| Vector ID | `{accession_number}:{transaction_index}` (same in both collections) |
| Chunk text | Deterministic template from `filing` ⋈ `form4_transaction` only (**no** Yahoo/SEC sentence in body) |
| Embeddings | **Two models, two Chroma collections** (never mix vectors in one collection) |
| 1 | `all-MiniLM-L6-v2` (local), dim **384**, collection `lestrade_txn_minilm` |
| 2 | `voyage-finance-2` (API), dim **1024**, collection `lestrade_txn_voyage` |
| Vector store | **Chroma**, persist dir on **Docker volume** (e.g. `/opt/airflow/chroma`) |
| Bookkeeping | Postgres **`embedding_index_state`** per `(accession, transaction_index, model_id)` |
| Backfill scope | Transactions for issuers in **`universe_snapshot`** for chosen `trading_date`(s) |
| `universe_group` metadata | **Option 3** (see below) |
| Out of scope v1 | Pre-chunk LLM summarization, contextual chunking (LLM), knowledge graph |

---

## Multi-entity without a graph

Relations are already explicit in Postgres (issuer, insider, filing, transaction, universe session). Phase 4 uses:

- **SQL** for filters, counts, thresholds, and universe membership.
- **Vectors** for fuzzy / narrative retrieval after metadata filters.

A graph DB is deferred until cross-document multi-hop reasoning is required.

---

## `universe_group` metadata (Option 3)

When joining a transaction to `universe_snapshot` for embed metadata:

1. If the issuer’s ticker has **exactly one** `universe_group` row for that `trading_date` → set `universe_group` to `high` or `low`.
2. If the ticker has **both** `high` and `low` rows on that date → set **`universe_group` to null** (omit or explicit null in metadata); still set `universe_trading_date`.
3. Phase 4 filters like “low list only” for ambiguous tickers should use **SQL** on `universe_snapshot`, not vector metadata alone.

Check for duplicates:

```sql
SELECT trading_date, ticker, array_agg(universe_group ORDER BY universe_group) AS groups
FROM universe_snapshot
GROUP BY trading_date, ticker
HAVING count(*) > 1;
```

---

## Components

```text
Postgres                          Chroma (Docker volume)
────────                          ──────────────────────
filing                            lestrade_txn_minilm  (384)
form4_transaction        ──►      lestrade_txn_voyage  (1024)
universe_snapshot                 same IDs, same text/metadata
embedding_index_state
```

### Chunk builder

- SQL: in-scope rows (universe-scoped `trading_date` list).
- `content_hash = hash(template_version + canonical_text)`.
- Metadata: accession, indices, CIK, ticker, insider, dates, codes, `is_amendment`, `universe_trading_date`, `universe_group` (nullable per Option 3).

### Embedding providers

| `model_id` | Provider | Notes |
|------------|----------|--------|
| `all-MiniLM-L6-v2` | Local `sentence-transformers` | Index: `input_type` N/A |
| `voyage-finance-2` | Voyage API | Index: `input_type=document`; query: `input_type=query` |

### `embedding_index_state`

| Column | Purpose |
|--------|---------|
| `accession_number`, `transaction_index`, `model_id` | PK |
| `content_hash` | Skip if unchanged |
| `chroma_collection` | Target collection name |
| `status`, `error_message`, `embedded_at` | Observability |

### Airflow DAG

- **`embed_form4_transactions`**: tasks or param for MiniLM then Voyage; batch upsert; commit per batch; pool optional (MiniLM local; Voyage HTTP).

---

## Environment variables (planned)

| Variable | Purpose |
|----------|---------|
| `LESTRADE_CHROMA_PERSIST_DIR` | Chroma path (Docker volume) |
| `LESTRADE_EMBEDDING_MODEL` | `all-MiniLM-L6-v2` or `voyage-finance-2` |
| `LESTRADE_UNIVERSE_TRADING_DATE` | Single session backfill (optional) |
| `LESTRADE_EMBED_TRADING_DATES` | Comma-separated dates for week backfill (optional) |
| `VOYAGE_API_KEY` | Voyage embed API |
| `LESTRADE_EMBED_BATCH_SIZE` | Batch size (default TBD in code) |

---

## Implementation order

1. Migration **`008_embedding_index_state.sql`**
2. Package **`src/lestrade_embed/`** (template, metadata, providers, chroma store)
3. Unit tests (template, Option 3 metadata, content_hash; no live API in CI)
4. DAG **`embed_form4_transactions`**
5. Docker volume + `docker-compose.yml` / `airflow.env.example`
6. **`scripts/eval_retrieval.py`** (fixed question set; both collections)
7. Runbook section in **`PHASE1_RUNBOOK.md`**

---

## Phase 4 handoff

- Query uses **same `model_id` and collection** as index.
- Structured questions → SQL; fuzzy → metadata filter + vector search + LLM with accession citations.
