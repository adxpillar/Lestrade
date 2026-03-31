"""
Parameterized Form 4 backfill (date range, optional CIK filter, optional submissions path).

Trigger with **JSON config** (e.g. Airflow UI → Trigger DAG w/ config):

.. code-block:: json

    {
      "start_date": "2024-03-01",
      "end_date": "2024-03-05",
      "cik_list": ["0000320193"],
      "use_submissions": false
    }

- ``use_submissions``: if true and ``cik_list`` is non-empty, uses ``data.sec.gov``
  submissions for those CIKs (``filings.recent`` only — limited history). Otherwise
  uses daily ``master.idx`` per day (full market for that date), optionally filtered
  by ``cik_list``.

Uses pool **edgar_http**; create it in Admin → Pools (~4 slots). Same env vars as
``edgar_daily_incremental``.
"""

from __future__ import annotations

import os
from datetime import date, datetime
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


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

    app = os.environ["EDGAR_APP_NAME"]
    email = os.environ["EDGAR_CONTACT_EMAIL"]
    raw_mode = os.environ.get("LESTRADE_RAW_STORAGE", "bytea").lower()
    s3_client = None
    bucket = None
    if raw_mode == "s3":
        import boto3

        bucket = os.environ["LESTRADE_S3_BUCKET"]
        s3_client = boto3.client("s3")

    hook = PostgresHook(postgres_conn_id=os.environ.get("LESTRADE_PG_CONN", "lestrade_rds"))
    conn = hook.get_conn()
    try:
        client = EdgarClient(app_name=app, contact_email=email)
        cfg = IngestConfig(
            raw_storage=raw_mode,  # type: ignore[arg-type]
            s3_client=s3_client,
            s3_bucket=bucket,
        )
        if use_submissions and cik_list:
            ingest_cik_submissions_for_date_range(conn, client, cik_list, start, end, cfg)
        else:
            ingest_date_range(conn, client, start, end, cfg, cik_filter=cik_list)
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
    },
) as dag:
    PythonOperator(
        task_id="backfill_form4_range",
        python_callable=_run_backfill,
        pool="edgar_http",
    )
