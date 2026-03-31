"""
Daily Form 4 incremental ingest (US/Eastern “yesterday” filing date).

Prerequisites
-------------
1. Airflow **Connection** ``lestrade_rds`` (Postgres) — same as smoke test DAG.
2. Create pool **edgar_http** with ~4 slots (Admin → Pools) to cap concurrent EDGAR work.
3. MWAA **Requirements** must install ``lestrade-edgar`` with ``[ingest]`` (and ``[s3]`` if used).
4. Environment variables on the worker (or Airflow Variables):
   - ``EDGAR_APP_NAME`` — short app name for SEC User-Agent
   - ``EDGAR_CONTACT_EMAIL`` — contact email for SEC User-Agent
   - ``LESTRADE_RAW_STORAGE`` — ``bytea`` (default) or ``s3``
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def _ingest_yesterday() -> None:
    from lestrade_edgar.client import EdgarClient
    from lestrade_ingest import IngestConfig, eastern_previous_calendar_date, ingest_date_range

    app = os.environ["EDGAR_APP_NAME"]
    email = os.environ["EDGAR_CONTACT_EMAIL"]
    raw_mode = os.environ.get("LESTRADE_RAW_STORAGE", "bytea").lower()
    if raw_mode not in ("bytea", "s3"):
        raise ValueError("LESTRADE_RAW_STORAGE must be bytea or s3")

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
        day = eastern_previous_calendar_date()
        cfg = IngestConfig(
            raw_storage=raw_mode,  # type: ignore[arg-type]
            s3_client=s3_client,
            s3_bucket=bucket,
        )
        ingest_date_range(conn, client, day, day, cfg)
    finally:
        conn.close()


with DAG(
    dag_id="edgar_daily_incremental",
    start_date=datetime(2025, 1, 1),
    schedule="0 7 * * *",
    catchup=False,
    tags=["lestrade", "edgar", "ingest"],
    max_active_runs=1,
) as dag:
    PythonOperator(
        task_id="ingest_yesterday_form4",
        python_callable=_ingest_yesterday,
        pool="edgar_http",
    )
