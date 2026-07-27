"""
Parameterized Form 4 backfill (date range, optional CIK filter, optional submissions path).

Trigger with **JSON config** (e.g. Airflow UI → Trigger DAG w/ config):

.. code-block:: json

    {
      "start_date": "2024-03-01",
      "end_date": "2024-03-05",
      "cik_list": ["0000320193"],
      "use_submissions": false,
      "force_refetch": false,
      "force_reparse": false
    }

- ``use_submissions``: if true and ``cik_list`` is non-empty, uses ``data.sec.gov``
  submissions for those CIKs (``filings.recent`` only — limited history). Otherwise
  uses daily ``master.idx`` per day (full market for that date), optionally filtered
  by ``cik_list``.
- ``force_refetch``: if true, downloads from EDGAR even if raw bytes already exist.
- ``force_reparse``: if true, reparses and reloads even if parsed rows already exist.

Uses pool **edgar_http**; create it in Admin → Pools (~4 slots). Same configuration
as ``edgar_daily_incremental`` (environment variables and/or Airflow Variables).
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from lestrade_airflow_env import (
    edgar_user_agent_from_env_or_variables,
    optional_edgar_user_agent_override,
    postgres_conn_id_from_env_or_variable,
    str_from_env_or_variable,
)


def _parse_cik_list(raw: Any) -> list[str] | None:
    if raw is None:
        return None
    if isinstance(raw, str):
        return [c.strip() for c in raw.split(",") if c.strip()]
    if isinstance(raw, list):
        return [str(c).strip() for c in raw if str(c).strip()]
    return None


def _run_backfill(**context: Any) -> None:
    from lestrade_edgar.client import EdgarClient
    from lestrade_ingest import (
        IngestConfig,
        ingest_cik_submissions_for_date_range,
        ingest_date_range,
    )
    import logging

    log = logging.getLogger(__name__)

    params = context["params"]
    conf = context["dag_run"].conf or {}
    start_s = conf.get("start_date") or params["start_date"]
    end_s = conf.get("end_date") or params["end_date"]
    start = date.fromisoformat(str(start_s))
    end = date.fromisoformat(str(end_s))
    if end < start:
        raise ValueError("end_date must be >= start_date")

    cik_list = _parse_cik_list(conf.get("cik_list", params.get("cik_list")))
    use_submissions = bool(conf.get("use_submissions", params.get("use_submissions")))
    force_refetch = bool(conf.get("force_refetch", params.get("force_refetch")))
    force_reparse = bool(conf.get("force_reparse", params.get("force_reparse")))

    app, email = edgar_user_agent_from_env_or_variables()
    raw_mode = str_from_env_or_variable(
        "LESTRADE_RAW_STORAGE", "lestrade_raw_storage", default="bytea"
    ).lower()
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
        client = EdgarClient(
            app_name=app,
            contact_email=email,
            user_agent=optional_edgar_user_agent_override(),
            # Keep request rate conservative to avoid SEC edge/WAF 403 blocks.
            min_interval_s=0.5,
            max_attempts=10,
            base_backoff_s=5.0,
            max_backoff_s=300.0,
        )
        cfg = IngestConfig(
            raw_storage=raw_mode,  # type: ignore[arg-type]
            s3_client=s3_client,
            s3_bucket=bucket,
            force_refetch=force_refetch,
            force_reparse=force_reparse,
        )
        if use_submissions and cik_list:
            counts = ingest_cik_submissions_for_date_range(
                conn, client, cik_list, start, end, cfg
            )
        else:
            counts = ingest_date_range(conn, client, start, end, cfg, cik_filter=cik_list)
        log.info(
            "EDGAR backfill counts start=%s end=%s use_submissions=%s cik_list=%s: %s",
            start.isoformat(),
            end.isoformat(),
            use_submissions,
            cik_list,
            counts,
        )
    finally:
        conn.close()


with DAG(
    dag_id="edgar_backfill",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lestrade", "edgar", "ingest", "backfill"],
    max_active_runs=1,
    params={
        "start_date": "2025-01-01",
        "end_date": "2025-01-01",
        "cik_list": None,
        "use_submissions": False,
        "force_refetch": False,
        "force_reparse": False,
    },
) as dag:
    PythonOperator(
        task_id="backfill_form4_range",
        python_callable=_run_backfill,
        pool="edgar_http",
    )
