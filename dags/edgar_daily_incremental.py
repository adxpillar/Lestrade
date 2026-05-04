"""
Daily Form 4 incremental ingest (US/Eastern “yesterday” filing date).

Prerequisites
-------------
1. Airflow **Connection** ``lestrade_rds`` (Postgres) — same as smoke test DAG.
2. Create pool **edgar_http** with ~4 slots (Admin → Pools) to cap concurrent EDGAR work.
3. MWAA **Requirements** must install ``lestrade-edgar`` with ``[ingest]`` (and ``[s3]`` if used).
4. **EDGAR User-Agent** (environment variable and/or Airflow Variable — same logical value):
   - ``EDGAR_APP_NAME`` or Variable ``edgar_app_name`` — short app name for SEC
   - ``EDGAR_CONTACT_EMAIL`` or Variable ``edgar_contact_email`` — contact email for SEC
   - ``LESTRADE_RAW_STORAGE`` or Variable ``lestrade_raw_storage`` — ``bytea`` (default) or ``s3``
   - If raw storage is ``s3``: ``LESTRADE_S3_BUCKET`` or Variable ``lestrade_s3_bucket``
   - Optional: ``LESTRADE_PG_CONN`` (default connection id ``lestrade_rds``; env only — avoids noisy Variable 404 logs on AF3)
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from lestrade_airflow_env import (
    edgar_user_agent_from_env_or_variables,
    optional_edgar_user_agent_override,
    postgres_conn_id_from_env_or_variable,
    str_from_env_or_variable,
)


def _ingest_yesterday() -> None:
    from lestrade_edgar.client import EdgarClient
    from lestrade_ingest import IngestConfig, eastern_previous_calendar_date, ingest_date_range
    import logging

    log = logging.getLogger(__name__)

    app, email = edgar_user_agent_from_env_or_variables()
    raw_mode = str_from_env_or_variable(
        "LESTRADE_RAW_STORAGE", "lestrade_raw_storage", default="bytea"
    ).lower()
    if raw_mode not in ("bytea", "s3"):
        raise ValueError("LESTRADE_RAW_STORAGE must be bytea or s3")

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
            # SEC edge/WAF can return 403 during throttling; keep aggregate rate low.
            # With pool edgar_http=4, 0.5s implies ~8 req/s max across concurrent tasks.
            min_interval_s=0.5,
            max_attempts=10,
            base_backoff_s=5.0,
            max_backoff_s=300.0,
        )
        day = eastern_previous_calendar_date()
        cfg = IngestConfig(
            raw_storage=raw_mode,  # type: ignore[arg-type]
            s3_client=s3_client,
            s3_bucket=bucket,
        )
        counts = ingest_date_range(conn, client, day, day, cfg)
        log.info("EDGAR ingest counts for %s: %s", day.isoformat(), counts)
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
