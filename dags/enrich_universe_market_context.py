"""
Phase 2 — market enrichment for the covered universe.

Workflow:
- Run after `universe_snapshot` and `edgar_daily_incremental`.
- Uses `LESTRADE_UNIVERSE_TRADING_DATE` (ISO) to select universe tickers for that session.
- Fetches daily price history (Stooq) for those tickers and upserts into `security_daily_prices`.
- Computes simple derived metrics and upserts into `universe_market_context`.

Env/Variable knobs:
- LESTRADE_UNIVERSE_TRADING_DATE (required; must exist in universe_snapshot)
- Optional: LESTRADE_PG_CONN (defaults to lestrade_rds)
- Optional: LESTRADE_ENRICH_MARKET_TIMEOUT_HOURS (default 8; Airflow task execution cap)
- Stooq may enforce a **daily hits limit**; the DAG stops fetching further tickers but still upserts
  `universe_market_context` for whatever prices are already cached (task **succeeds**).
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from lestrade_airflow_env import postgres_conn_id_from_env_or_variable, str_from_env_or_variable


def _enrich_market_context() -> None:
    import logging

    import requests

    from lestrade_enrich.market import (
        compute_universe_market_context,
        upsert_security_daily_prices,
        upsert_universe_market_context,
    )
    from lestrade_enrich.stooq import fetch_daily_bars_stooq
    from lestrade_ingest.universe import normalize_ticker, tickers_for_date, universe_snapshot_exists_for_date

    log = logging.getLogger(__name__)

    trading_date_s = str_from_env_or_variable(
        "LESTRADE_UNIVERSE_TRADING_DATE",
        "lestrade_universe_trading_date",
        default="",
    ).strip()
    if not trading_date_s:
        raise ValueError("LESTRADE_UNIVERSE_TRADING_DATE is required (YYYY-MM-DD)")
    trading_date = date.fromisoformat(trading_date_s[:10])

    stooq_api_key = str_from_env_or_variable(
        "LESTRADE_STOOQ_APIKEY",
        "lestrade_stooq_apikey",
        default="",
    ).strip()
    stooq_api_key = stooq_api_key or None

    hook = PostgresHook(postgres_conn_id=postgres_conn_id_from_env_or_variable())
    conn = hook.get_conn()
    try:
        if not universe_snapshot_exists_for_date(conn, trading_date):
            raise ValueError(
                f"No universe_snapshot rows for {trading_date.isoformat()}. "
                "Run DAG `universe_snapshot` first."
            )

        groups = tickers_for_date(conn, trading_date)
        tickers = sorted(set(groups["high"]) | set(groups["low"]))
        if not tickers:
            log.info("No tickers for %s; skipping market enrichment.", trading_date.isoformat())
            return

        # Fetch and cache prices. (Stooq returns full history; we store all and compute locally.)
        sess = requests.Session()
        upserted = 0
        skipped_cached = 0
        hit_stooq_daily_limit = False
        for t in tickers:
            nt = normalize_ticker(t)
            # Skip tickers we already have any cached bars for. This makes reruns cheap and
            # avoids burning Stooq hit quota repeatedly.
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM security_daily_prices WHERE ticker = %s LIMIT 1",
                    (nt,),
                )
                if cur.fetchone():
                    skipped_cached += 1
                    continue
            try:
                bars = fetch_daily_bars_stooq(nt, api_key=stooq_api_key, session=sess)
            except Exception as e:
                # Stooq quota: stop fetching but still compute context from whatever is already cached.
                if "daily hits limit" in str(e).lower():
                    conn.commit()
                    hit_stooq_daily_limit = True
                    log.error(
                        "Stooq daily hits limit reached at ticker %s; stopping price fetches. "
                        "Rerun tomorrow or after quota resets. Partial rows committed.",
                        t,
                    )
                    break
                log.warning("Price fetch failed for %s: %s", t, e)
                continue
            upserted += upsert_security_daily_prices(
                conn,
                ticker=nt,
                bars=[
                    {
                        "price_date": b.price_date,
                        "open": b.open,
                        "high": b.high,
                        "low": b.low,
                        "close": b.close,
                        "volume": b.volume,
                    }
                    for b in bars
                ],
            )
            # Commit per ticker so hitting Stooq quota doesn't rollback progress.
            conn.commit()

        if upserted == 0 and skipped_cached == 0 and not hit_stooq_daily_limit:
            raise ValueError(
                "Market enrichment ingested zero price rows. "
                "Stooq may require an API key; set `LESTRADE_STOOQ_APIKEY` "
                "(or Airflow Variable `lestrade_stooq_apikey`) and rerun."
            )
        if skipped_cached == len(tickers) and upserted == 0:
            log.info(
                "All %s universe tickers already had cached prices; computing/updating context only.",
                len(tickers),
            )

        ctx = compute_universe_market_context(conn, trading_date)
        n_ctx = upsert_universe_market_context(conn, ctx)
        conn.commit()
        log.info(
            "Market enrichment done for %s: tickers=%s price_rows_upserted=%s "
            "skipped_cached=%s stooq_daily_limit_stop=%s context_rows=%s",
            trading_date.isoformat(),
            len(tickers),
            upserted,
            skipped_cached,
            hit_stooq_daily_limit,
            n_ctx,
        )
    finally:
        conn.close()


with DAG(
    dag_id="enrich_universe_market_context",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lestrade", "enrich", "market"],
    max_active_runs=1,
) as dag:
    enrich_timeout_s = str_from_env_or_variable(
        "LESTRADE_ENRICH_MARKET_TIMEOUT_HOURS",
        "lestrade_enrich_market_timeout_hours",
        default="8",
    ).strip()
    try:
        enrich_timeout_h = int(enrich_timeout_s)
    except ValueError:
        enrich_timeout_h = 8
    enrich_timeout_h = max(1, min(enrich_timeout_h, 24))

    PythonOperator(
        task_id="enrich_market_context",
        python_callable=_enrich_market_context,
        pool="edgar_http",  # conservative; both EDGAR and market data should be throttled
        execution_timeout=timedelta(hours=enrich_timeout_h),
    )

