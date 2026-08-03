# SEC Form 4 insider trading RAG

End-to-end pipeline and application for ingesting SEC **Form 4** insider trading filings from EDGAR, enriching them with market context, embedding transaction-level text for semantic search, and answering natural-language questions with **hybrid structured + RAG** retrieval and citations.

## Product vision

The intended **web application** showcases **browseable highs and lows** ticker lists sourced from this daily universe. Users pick symbols they care about from the UI and ask **natural-language questions** grounded in filings and enrichment. The positioning is discovery: tying **recent market structure** (new highs/lows) to **insider behavior** to surface interesting leads—not personalized investment advice (see disclaimer).

## Documentation

| Document | Description |
|----------|-------------|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | System architecture, data flow, hybrid query model, and technology choices |
| [docs/PHASES.md](docs/PHASES.md) | Phased implementation plan (ingestion → enrichment → embeddings → RAG → UI) |
| [docs/DATA_CONTRACT.md](docs/DATA_CONTRACT.md) | Phase 1 data contract, tables, and EDGAR + MWAA + RDS strategy |
| [docs/PHASE1_RUNBOOK.md](docs/PHASE1_RUNBOOK.md) | DAG order, env vars, monitoring SQL, validation checklist, and incident notes |
| [docs/PHASE2_DATA.md](docs/PHASE2_DATA.md) | Phase 2 enrichment tables: field → source (Stooq, yfinance) |
| [docs/PHASE3_ARCHITECTURE.md](docs/PHASE3_ARCHITECTURE.md) | Phase 3 embeddings: dual models, Chroma, scope, metadata rules |
| [docs/UI_OLLAMA.md](docs/UI_OLLAMA.md) | Phase 4/5 demo: MiniLM retrieval + Ollama chat + Streamlit |

## Run the chat UI (MiniLM + Ollama, no Voyage)

See **[docs/UI_OLLAMA.md](docs/UI_OLLAMA.md)**. Short version:

1. Install and start [Ollama](https://ollama.com), then `ollama pull llama3.2`
2. Point `LESTRADE_CHROMA_PERSIST_DIR` at `./data/chroma` (Compose bind-mounts this for Airflow)
3. `uv sync --extra ui` then `uv run streamlit run app/streamlit_app.py`

## Run Airflow locally (Docker) + Supabase Postgres

To run the ingestion DAGs without AWS MWAA/RDS:

- **Airflow on Docker**: see `docker/airflow/README.md`
- **Database**: apply **`db/migrations/`** in order (see **`db/README.md`**); field semantics in **`docs/DATA_CONTRACT.md`**
- **Operations**: **`docs/PHASE1_RUNBOOK.md`** (monitoring queries, SEC rate limits vs implementation, validation)
- **Connection**: configure an Airflow Connection id **`lestrade_rds`** pointing at Supabase (details in `docker/airflow/README.md`)

## Covered universe (Barchart CSV input)

For a given stamped session (**`LESTRADE_UNIVERSE_TRADING_DATE=YYYY-MM-DD`**), every ticker in **both** of the paired Barchart exports is assumed to qualify as either a **3‑month new high** or **3‑month new low** across **US exchanges**, as of that snapshot (your export provider’s methodology applies). The ingestion layer does **not** re-score the screen—it loads **whatever symbols** appear in those two files.

1. Export or download the highs and lows reports for that session and place **exactly one** of each under **`barchart_report_today/`** at the repo root (or set **`LESTRADE_BARCHART_DIR`** to another absolute path readable by workers).
2. File names **must** follow Barchart’s pattern; the **`MM-DD-YYYY`** suffix in **both** names is parsed and **must refer to the same calendar session** (the DAG errors if highs and lows disagree):
   - **`all-us-exchanges-3-month-new-highs-MM-DD-YYYY.csv`**
   - **`all-us-exchanges-3-month-new-lows-MM-DD-YYYY.csv`**

   Example:
   - `all-us-exchanges-3-month-new-highs-05-06-2026.csv`
   - `all-us-exchanges-3-month-new-lows-05-06-2026.csv` → session **2026-05-06**.

3. Optional but recommended: set **`LESTRADE_UNIVERSE_TRADING_DATE=YYYY-MM-DD`** in `.env` (or Variable `lestrade_universe_trading_date`). If set, it **must** match the date encoded in both filenames—otherwise `universe_snapshot` fails fast.
4. Run DAG **`universe_snapshot`** (derives the trading date from the filenames, resolves ticker→CIK, persists `universe_snapshot`).
5. Set **`LESTRADE_UNIVERSE_TRADING_DATE`** to that same ISO date (if you did not set it before snapshot) and run **`edgar_daily_incremental`** and optionally **`edgar_backfill_on_entry`**.

With Docker Compose, the repo mounts at **`/opt/airflow/lestrade`**, so workers default to **`/opt/airflow/lestrade/barchart_report_today`** when `LESTRADE_BARCHART_DIR` is unset.

After a successful `universe_snapshot`, the DAG **archives** the processed highs/lows CSVs into
**`barchart_report_archive/YYYY-MM-DD/`** (override with `LESTRADE_BARCHART_ARCHIVE_DIR`).

## Disclaimer

This project is for research and engineering demonstration. SEC EDGAR data should be used in accordance with SEC policies. Nothing here is investment advice.
