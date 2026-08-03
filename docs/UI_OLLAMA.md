# Streamlit demo — MiniLM + Ollama

Low-cost Phase 4/5 path: **retrieve with MiniLM**, **answer with local Ollama**. Voyage is not required.

## Prerequisites

1. **MiniLM index** already built (`embed_form4_transactions` / `embed_minilm` succeeded).
2. **Ollama** on your Mac: [https://ollama.com](https://ollama.com)
3. Pull a small chat model:

```bash
ollama pull llama3.2
```

4. Chroma data under **`./data/chroma`** (Compose bind-mounts this to `/opt/airflow/chroma`).

If your embeddings still live only in the old Docker volume `lestrade_lestrade-chroma`, copy them once:

```bash
mkdir -p data/chroma
docker run --rm \
  -v lestrade_lestrade-chroma:/from \
  -v "$(pwd)/data/chroma:/to" \
  alpine sh -c "cp -a /from/. /to/ && chown -R 50000:0 /to"
```

Then recreate Airflow so it uses the bind mount:

```bash
docker compose up -d
```

## Env

In `.env` (or export in the shell):

```bash
LESTRADE_CHROMA_PERSIST_DIR=./data/chroma   # host path for Streamlit
LESTRADE_OLLAMA_BASE_URL=http://127.0.0.1:11434
LESTRADE_OLLAMA_MODEL=llama3.2
# Universe sidebar (optional but useful):
# LESTRADE_DATABASE_URL=postgresql://...  # or reuse AIRFLOW_CONN_LESTRADE_RDS
```

Airflow containers still see Chroma at `/opt/airflow/chroma` via the same `./data/chroma` folder.

## Run Streamlit

```bash
cd /path/to/Lestrade
uv sync --extra ui
uv run streamlit run app/streamlit_app.py
```

Open the URL Streamlit prints (usually `http://localhost:8501`).

## What the UI does

- Sidebar: universe session / highs-lows / ticker / buy-sell filters (from Postgres when configured)
- Chat: MiniLM vector search → Ollama answers **only from retrieved Form 4 excerpts**
- Sources: accession + EDGAR link when CIK is present

## Embed DAG without Voyage

Default (after this change): set `LESTRADE_EMBED_SKIP_VOYAGE=1` (or leave unset — DAG defaults to skipping Voyage). To run Voyage again:

```bash
LESTRADE_EMBED_SKIP_VOYAGE=0
```

## Smoke retrieval (no LLM)

```bash
export LESTRADE_CHROMA_PERSIST_DIR=./data/chroma
uv run python scripts/eval_retrieval.py
```

## Cost note

| Component | Cost |
|-----------|------|
| MiniLM | Free (local) |
| Ollama | Free (local) |
| Streamlit local | Free |
| Voyage | Not used |
