"""
Phase 3 — embed Form 4 transactions for universe-scoped session date(s).

Runs MiniLM (local) then Voyage (API) into separate Chroma collections.
Requires migration ``008_embedding_index_state`` and ``[embed]`` package extra.

Env:
- LESTRADE_UNIVERSE_TRADING_DATE or LESTRADE_EMBED_TRADING_DATES (comma-separated)
- LESTRADE_CHROMA_PERSIST_DIR (Docker volume / bind mount path)
- LESTRADE_EMBED_SKIP_VOYAGE (default 1 — MiniLM only; set 0 to run Voyage)
- VOYAGE_API_KEY (Voyage task only)
- LESTRADE_EMBED_BATCH_SIZE (optional, default 32)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from lestrade_airflow_env import postgres_conn_id_from_env_or_variable, str_from_env_or_variable


def _run_embed(*, model_id: str) -> None:
    import logging

    from lestrade_embed.dates import resolve_embed_trading_dates
    from lestrade_embed.sync import sync_embeddings_for_model

    log = logging.getLogger(__name__)
    trading_dates = resolve_embed_trading_dates(
        universe_trading_date=str_from_env_or_variable(
            "LESTRADE_UNIVERSE_TRADING_DATE",
            "lestrade_universe_trading_date",
            default="",
        ),
        embed_trading_dates=str_from_env_or_variable(
            "LESTRADE_EMBED_TRADING_DATES",
            "lestrade_embed_trading_dates",
            default="",
        ),
    )
    hook = PostgresHook(postgres_conn_id=postgres_conn_id_from_env_or_variable())

    def _reconnect():
        # Fresh connection after pooler idle/timeout drops (common on Supabase :6543).
        return hook.get_conn()

    conn = _reconnect()
    try:
        stats = sync_embeddings_for_model(
            conn,
            model_id=model_id,
            trading_dates=trading_dates,
            reconnect=_reconnect,
        )
        log.info(
            "embed_form4_transactions model=%s dates=%s stats=%s",
            model_id,
            [d.isoformat() for d in trading_dates],
            stats,
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass


with DAG(
    dag_id="embed_form4_transactions",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lestrade", "embed"],
    max_active_runs=1,
) as dag:
    timeout_s = str_from_env_or_variable(
        "LESTRADE_EMBED_TIMEOUT_HOURS",
        "lestrade_embed_timeout_hours",
        default="4",
    ).strip()
    try:
        timeout_h = int(timeout_s)
    except ValueError:
        timeout_h = 4
    timeout_h = max(1, min(timeout_h, 24))

    embed_minilm = PythonOperator(
        task_id="embed_minilm",
        python_callable=_run_embed,
        op_kwargs={"model_id": "all-MiniLM-L6-v2"},
        execution_timeout=timedelta(hours=timeout_h),
    )
    skip_voyage = str_from_env_or_variable(
        "LESTRADE_EMBED_SKIP_VOYAGE",
        "lestrade_embed_skip_voyage",
        default="1",
    ).strip().lower() in ("1", "true", "yes", "y")

    if skip_voyage:
        # MiniLM-only (default): enough for the local Ollama Streamlit demo.
        embed_minilm
    else:
        embed_voyage = PythonOperator(
            task_id="embed_voyage",
            python_callable=_run_embed,
            op_kwargs={"model_id": "voyage-finance-2"},
            execution_timeout=timedelta(hours=timeout_h),
        )
        embed_minilm >> embed_voyage
