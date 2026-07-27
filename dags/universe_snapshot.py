"""
Universe snapshot from Barchart CSV exports.

Place **exactly one** highs and **exactly one** lows file under ``barchart_report_today/``
(override with ``LESTRADE_BARCHART_DIR``), named:

  - ``all-us-exchanges-3-month-new-highs-MM-DD-YYYY.csv``
  - ``all-us-exchanges-3-month-new-lows-MM-DD-YYYY.csv``

The **session date** is taken from the **MM-DD-YYYY** in both filenames; they must agree.
Optional: set ``LESTRADE_UNIVERSE_TRADING_DATE`` (ISO ``YYYY-MM-DD``)—if set, it must match
the filename-derived date (cross-check for other DAGs that read env).

This DAG:
  - reads tickers from both CSVs
  - resolves tickers -> CIK using SEC ``company_tickers.json``
  - upserts ``security_master`` and ``universe_snapshot`` (high / low)
"""

from __future__ import annotations

import logging
import shutil
from datetime import date, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from lestrade_airflow_env import (
    edgar_user_agent_from_env_or_variables,
    optional_edgar_user_agent_override,
    postgres_conn_id_from_env_or_variable,
    str_from_env_or_variable,
)
from lestrade_edgar.client import build_sec_user_agent

log = logging.getLogger(__name__)


def _resolve_tickers_to_ciks(tickers: list[str], *, user_agent: str) -> dict[str, str | None]:
    import json
    import requests

    if not tickers:
        return {}

    url = "https://www.sec.gov/files/company_tickers.json"
    resp = requests.get(url, headers={"User-Agent": user_agent}, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    mapping: dict[str, str] = {}
    for rec in data.values():
        t = str(rec.get("ticker", "")).upper().strip()
        cik = rec.get("cik_str")
        if not t or cik is None:
            continue
        try:
            cik_i = int(cik)
        except Exception:
            continue
        mapping[t] = str(cik_i).zfill(10)

    def _lookup(t: str) -> str | None:
        tt = str(t or "").upper().strip()
        if not tt:
            return None
        # Common class-share variants: some sources use "." while SEC uses "-" (or vice versa).
        return (
            mapping.get(tt)
            or mapping.get(tt.replace(".", "-"))
            or mapping.get(tt.replace("-", "."))
        )

    out: dict[str, str | None] = {}
    for t in tickers:
        out[t] = _lookup(t)
    return out


def _snapshot_universe() -> None:
    from lestrade_ingest.universe import (
        UniverseRow,
        resolve_barchart_csv_pair,
        resolve_barchart_archive_dir,
        resolve_barchart_report_dir,
        tickers_from_barchart_csv,
        upsert_security_master,
        upsert_universe_snapshot,
    )

    barchart_dir_hint = str_from_env_or_variable(
        "LESTRADE_BARCHART_DIR",
        "lestrade_barchart_dir",
        default="",
    )
    report_dir = resolve_barchart_report_dir(barchart_dir_hint or None)

    trading_date, high_path, low_path = resolve_barchart_csv_pair(report_dir)

    env_s = str_from_env_or_variable(
        "LESTRADE_UNIVERSE_TRADING_DATE",
        "lestrade_universe_trading_date",
        default="",
    ).strip()
    if env_s:
        env_date = date.fromisoformat(env_s[:10])
        if env_date != trading_date:
            raise ValueError(
                f"LESTRADE_UNIVERSE_TRADING_DATE is {env_date.isoformat()} but highs/lows "
                f"filenames encode {trading_date.isoformat()}."
            )
    else:
        log.info(
            "LESTRADE_UNIVERSE_TRADING_DATE unset; using %s from CSV filenames. "
            "Set the same ISO date in the environment for edgar_daily_incremental and "
            "edgar_backfill_on_entry.",
            trading_date.isoformat(),
        )

    tickers_high = tickers_from_barchart_csv(high_path)
    tickers_low = tickers_from_barchart_csv(low_path)

    app, email = edgar_user_agent_from_env_or_variables()
    user_agent = optional_edgar_user_agent_override() or build_sec_user_agent(app, email)

    all_tickers = sorted(set(tickers_high) | set(tickers_low))
    resolved = _resolve_tickers_to_ciks(all_tickers, user_agent=user_agent)

    hook = PostgresHook(postgres_conn_id=postgres_conn_id_from_env_or_variable())
    conn = hook.get_conn()
    try:
        upsert_security_master(conn, resolved)

        rows: list[UniverseRow] = []
        for t in tickers_high:
            rows.append(
                UniverseRow(
                    trading_date=trading_date,
                    ticker=t,
                    universe_group="high",
                    cik=resolved.get(t),
                )
            )
        for t in tickers_low:
            rows.append(
                UniverseRow(
                    trading_date=trading_date,
                    ticker=t,
                    universe_group="low",
                    cik=resolved.get(t),
                )
            )

        upsert_universe_snapshot(conn, rows)
        conn.commit()
    finally:
        conn.close()

    # Archive the processed CSVs so the drop folder stays "one highs + one lows".
    archive_dir_hint = str_from_env_or_variable(
        "LESTRADE_BARCHART_ARCHIVE_DIR",
        "lestrade_barchart_archive_dir",
        default="",
    )
    archive_root = resolve_barchart_archive_dir(archive_dir_hint or None)
    archive_day_dir = (archive_root / trading_date.isoformat()).resolve()
    archive_day_dir.mkdir(parents=True, exist_ok=True)

    for src in (high_path, low_path):
        dst = archive_day_dir / src.name
        if dst.exists():
            raise FileExistsError(
                f"Archive destination already exists: {dst}. "
                "Refusing to overwrite (possible re-run without cleanup)."
            )
        shutil.move(str(src), str(dst))
        log.info("Archived %s -> %s", src, dst)


with DAG(
    dag_id="universe_snapshot",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lestrade", "universe"],
    max_active_runs=1,
) as dag:
    PythonOperator(
        task_id="snapshot_universe",
        python_callable=_snapshot_universe,
        pool="edgar_http",
    )
