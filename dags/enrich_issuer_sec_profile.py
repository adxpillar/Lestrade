"""
Phase 2 — canonical issuer metadata from SEC company submissions (data.sec.gov).

Fetches ``submissions/CIK##########.json`` per CIK and upserts ``issuer_sec_profile``.

Scope (env ``LESTRADE_ISSUER_SEC_SCOPE`` or Variable ``lestrade_issuer_sec_scope``):

- ``universe`` (default): distinct CIKs from ``universe_snapshot`` for
  ``LESTRADE_UNIVERSE_TRADING_DATE`` (must be set; run after ``universe_snapshot``).
- ``security_master``: CIKs from ``security_master`` (non-null), capped by
  ``LESTRADE_ISSUER_SEC_SECURITY_MASTER_LIMIT`` (default 500). Use for backfills;
  respect SEC rate limits.

Uses the same EDGAR User-Agent env as ingest; pool ``edgar_http`` recommended.
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


def _enrich_issuer_sec_profile() -> None:
    import logging
    from datetime import date

    from lestrade_edgar.client import EdgarClient
    from lestrade_enrich.sec_issuer_profile import (
        distinct_ciks_from_security_master,
        distinct_ciks_from_universe_snapshot,
        enrich_issuer_sec_profiles,
    )
    from lestrade_ingest.universe import universe_snapshot_exists_for_date

    log = logging.getLogger(__name__)

    scope = str_from_env_or_variable(
        "LESTRADE_ISSUER_SEC_SCOPE",
        "lestrade_issuer_sec_scope",
        default="universe",
    ).strip().lower()
    if scope not in ("universe", "security_master"):
        raise ValueError("LESTRADE_ISSUER_SEC_SCOPE must be 'universe' or 'security_master'")

    hook = PostgresHook(postgres_conn_id=postgres_conn_id_from_env_or_variable())
    conn = hook.get_conn()
    try:
        if scope == "universe":
            trading_date_s = str_from_env_or_variable(
                "LESTRADE_UNIVERSE_TRADING_DATE",
                "lestrade_universe_trading_date",
                default="",
            ).strip()
            if not trading_date_s:
                raise ValueError(
                    "LESTRADE_UNIVERSE_TRADING_DATE is required when "
                    "LESTRADE_ISSUER_SEC_SCOPE=universe"
                )
            trading_date = date.fromisoformat(trading_date_s[:10])
            if not universe_snapshot_exists_for_date(conn, trading_date):
                raise ValueError(
                    f"No universe_snapshot rows for {trading_date.isoformat()}. "
                    "Run DAG `universe_snapshot` first."
                )
            ciks = distinct_ciks_from_universe_snapshot(conn, trading_date)
        else:
            lim_s = str_from_env_or_variable(
                "LESTRADE_ISSUER_SEC_SECURITY_MASTER_LIMIT",
                "lestrade_issuer_sec_security_master_limit",
                default="500",
            ).strip()
            try:
                lim = int(lim_s)
            except ValueError:
                lim = 500
            ciks = distinct_ciks_from_security_master(conn, limit=lim)

        if not ciks:
            log.info("issuer_sec_profile: no CIKs to fetch (scope=%s).", scope)
            return

        app, email = edgar_user_agent_from_env_or_variables()
        ua_override = optional_edgar_user_agent_override()
        client = EdgarClient(
            app_name=app,
            contact_email=email,
            user_agent=ua_override,
            min_interval_s=0.5,
        )
        ok, err = enrich_issuer_sec_profiles(conn, client, ciks, commit_each=True)
        conn.commit()
        log.info(
            "issuer_sec_profile enrichment done: scope=%s ciks=%s ok=%s error=%s",
            scope,
            len(ciks),
            ok,
            err,
        )
    finally:
        conn.close()


with DAG(
    dag_id="enrich_issuer_sec_profile",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lestrade", "enrich", "sec"],
    max_active_runs=1,
) as dag:
    timeout_s = str_from_env_or_variable(
        "LESTRADE_ENRICH_ISSUER_SEC_TIMEOUT_HOURS",
        "lestrade_enrich_issuer_sec_timeout_hours",
        default="4",
    ).strip()
    try:
        timeout_h = int(timeout_s)
    except ValueError:
        timeout_h = 4
    timeout_h = max(1, min(timeout_h, 12))

    PythonOperator(
        task_id="enrich_issuer_sec_profile",
        python_callable=_enrich_issuer_sec_profile,
        pool="edgar_http",
        execution_timeout=timedelta(hours=timeout_h),
    )
