"""
Backfill for newly-added universe entrants.

Policy:
- Each market-close universe snapshot (high/low tickers) is persisted in Postgres by `universe_snapshot`.
- This DAG compares today's snapshot to the previous snapshot and backfills only *new entrants*
  for a configurable lookback window (default: 365 days).

Env/Variable knobs:
- LESTRADE_UNIVERSE_TRADING_DATE — same ISO stamp as `universe_snapshot` / `edgar_daily_incremental` (not `max(trading_date)` in DB).
- LESTRADE_ENTRY_BACKFILL_DAYS (default 365)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from lestrade_airflow_env import (
    edgar_user_agent_from_env_or_variables,
    optional_edgar_user_agent_override,
    postgres_conn_id_from_env_or_variable,
    str_from_env_or_variable,
)


def _backfill_new_entrants() -> None:
    from datetime import date

    from lestrade_edgar.client import EdgarClient
    from lestrade_ingest import IngestConfig, ingest_cik_submissions_for_date_range
    from lestrade_ingest.universe import (
        ciks_for_date_and_tickers,
        new_entrant_tickers,
        universe_snapshot_exists_for_date,
    )
    import logging

    log = logging.getLogger(__name__)

    app, email = edgar_user_agent_from_env_or_variables()
    raw_mode = str_from_env_or_variable(
        "LESTRADE_RAW_STORAGE", "lestrade_raw_storage", default="bytea"
    ).lower()
    if raw_mode not in ("bytea", "s3"):
        raise ValueError("LESTRADE_RAW_STORAGE must be bytea or s3")

    lookback_days_s = str_from_env_or_variable(
        "LESTRADE_ENTRY_BACKFILL_DAYS",
        "lestrade_entry_backfill_days",
        default="365",
    )
    try:
        lookback_days = int(lookback_days_s)
    except ValueError as e:
        raise ValueError("LESTRADE_ENTRY_BACKFILL_DAYS must be an integer") from e
    lookback_days = max(1, min(lookback_days, 3650))

    s3_client = None
    bucket = None
    if raw_mode == "s3":
        import boto3

        bucket = str_from_env_or_variable("LESTRADE_S3_BUCKET", "lestrade_s3_bucket")
        if not bucket:
            raise ValueError(
                "S3 raw storage requires LESTRADE_S3_BUCKET or Airflow Variable lestrade_s3_bucket"
            )
        s3_client = boto3.client("s3")

    hook = PostgresHook(postgres_conn_id=postgres_conn_id_from_env_or_variable())
    conn = hook.get_conn()
    try:
        trading_date_s = str_from_env_or_variable(
            "LESTRADE_UNIVERSE_TRADING_DATE",
            "lestrade_universe_trading_date",
            default="",
        ).strip()
        if not trading_date_s:
            raise ValueError(
                "Set LESTRADE_UNIVERSE_TRADING_DATE (YYYY-MM-DD) to the universe snapshot "
                "date for entrant detection. Run DAG `universe_snapshot` for that date first."
            )
        trading_date = date.fromisoformat(trading_date_s[:10])
        if not universe_snapshot_exists_for_date(conn, trading_date):
            raise ValueError(
                f"No universe_snapshot rows for {trading_date.isoformat()}. "
                "Run DAG `universe_snapshot` with matching Barchart CSVs and date."
            )

        entrants = new_entrant_tickers(conn, trading_date)
        entrant_tickers = sorted(set(entrants["high"]) | set(entrants["low"]))
        if not entrant_tickers:
            log.info("No new entrants on %s; skipping entry backfill.", trading_date.isoformat())
            return

        ciks = ciks_for_date_and_tickers(conn, trading_date, entrant_tickers)
        if not ciks:
            log.warning(
                "New entrants exist for %s but none have resolved CIKs; cannot backfill entrants.",
                trading_date.isoformat(),
            )
            return

        client = EdgarClient(
            app_name=app,
            contact_email=email,
            user_agent=optional_edgar_user_agent_override(),
            min_interval_s=0.5,
            max_attempts=6,
            base_backoff_s=5.0,
            max_backoff_s=120.0,
        )
        cfg = IngestConfig(
            raw_storage=raw_mode,  # type: ignore[arg-type]
            s3_client=s3_client,
            s3_bucket=bucket,
        )

        start = trading_date - timedelta(days=lookback_days)
        end = trading_date
        counts = ingest_cik_submissions_for_date_range(conn, client, ciks, start, end, cfg)
        log.info(
            "Entry backfill done for trading_date=%s lookback_days=%s entrants=%s ciks=%s: %s",
            trading_date.isoformat(),
            lookback_days,
            len(entrant_tickers),
            len(ciks),
            counts,
        )
    finally:
        conn.close()


with DAG(
    dag_id="edgar_backfill_on_entry",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lestrade", "edgar", "ingest", "backfill", "universe"],
    max_active_runs=1,
) as dag:
    PythonOperator(
        task_id="backfill_new_entrants",
        python_callable=_backfill_new_entrants,
        pool="edgar_http",
        execution_timeout=timedelta(hours=3),
    )

