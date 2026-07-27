"""
Phase 2 — issuer profile enrichment (Yahoo Finance) for the covered universe.

Run after ``universe_snapshot`` (same ``LESTRADE_UNIVERSE_TRADING_DATE`` as other enrich DAGs).

Requires the Python extra ``[enrich]`` (``yfinance``). Docker Compose installs ``[ingest,enrich]``.

Env / Variable knobs:
- LESTRADE_UNIVERSE_TRADING_DATE (required)
- LESTRADE_YFINANCE_SLEEP_S — seconds between Yahoo calls (default 0.35)
- LESTRADE_YFINANCE_TIMEOUT_S — per-ticker request timeout hint (default 25)
- LESTRADE_ENRICH_PROFILE_TIMEOUT_HOURS — Airflow task cap (default 4)
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from lestrade_airflow_env import postgres_conn_id_from_env_or_variable, str_from_env_or_variable


def _enrich_company_profile() -> None:
    import logging

    try:
        import yfinance  # noqa: F401
    except ImportError as e:
        raise ImportError(
            "yfinance is required for this DAG. Install the package extra: "
            "`pip install -e .[enrich]` (Docker: `_PIP_ADDITIONAL_REQUIREMENTS` must include "
            "`[ingest,enrich]`)."
        ) from e

    from lestrade_enrich.company_profile import enrich_universe_company_profiles_for_date
    from lestrade_ingest.universe import universe_snapshot_exists_for_date

    log = logging.getLogger(__name__)

    trading_date_s = str_from_env_or_variable(
        "LESTRADE_UNIVERSE_TRADING_DATE",
        "lestrade_universe_trading_date",
        default="",
    ).strip()
    if not trading_date_s:
        raise ValueError("LESTRADE_UNIVERSE_TRADING_DATE is required (YYYY-MM-DD)")
    trading_date = date.fromisoformat(trading_date_s[:10])

    sleep_s = str_from_env_or_variable(
        "LESTRADE_YFINANCE_SLEEP_S",
        "lestrade_yfinance_sleep_s",
        default="0.35",
    ).strip()
    try:
        sleep_f = float(sleep_s)
    except ValueError:
        sleep_f = 0.35
    sleep_f = max(0.0, min(sleep_f, 5.0))

    timeout_s = str_from_env_or_variable(
        "LESTRADE_YFINANCE_TIMEOUT_S",
        "lestrade_yfinance_timeout_s",
        default="25",
    ).strip()
    try:
        timeout_f = float(timeout_s)
    except ValueError:
        timeout_f = 25.0
    timeout_f = max(5.0, min(timeout_f, 120.0))

    hook = PostgresHook(postgres_conn_id=postgres_conn_id_from_env_or_variable())
    conn = hook.get_conn()
    try:
        if not universe_snapshot_exists_for_date(conn, trading_date):
            raise ValueError(
                f"No universe_snapshot rows for {trading_date.isoformat()}. "
                "Run DAG `universe_snapshot` first."
            )
        ok_c, no_c, err_c = enrich_universe_company_profiles_for_date(
            conn,
            trading_date,
            sleep_s=sleep_f,
            timeout_s=timeout_f,
        )
        conn.commit()
        log.info(
            "Company profile enrichment done for %s: ok=%s no_data=%s error=%s (yahoo sleep=%ss)",
            trading_date.isoformat(),
            ok_c,
            no_c,
            err_c,
            sleep_f,
        )
    finally:
        conn.close()


with DAG(
    dag_id="enrich_universe_company_profile",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lestrade", "enrich", "company"],
    max_active_runs=1,
) as dag:
    timeout_s = str_from_env_or_variable(
        "LESTRADE_ENRICH_PROFILE_TIMEOUT_HOURS",
        "lestrade_enrich_profile_timeout_hours",
        default="4",
    ).strip()
    try:
        timeout_h = int(timeout_s)
    except ValueError:
        timeout_h = 4
    timeout_h = max(1, min(timeout_h, 12))

    PythonOperator(
        task_id="enrich_company_profile",
        python_callable=_enrich_company_profile,
        execution_timeout=timedelta(hours=timeout_h),
    )
