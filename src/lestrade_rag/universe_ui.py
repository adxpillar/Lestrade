from __future__ import annotations

import os
from datetime import date
from typing import Any

import psycopg2


def database_url() -> str:
    """Prefer Lestrade DB URL used by Airflow env, else LESTRADE_DATABASE_URL."""
    for key in ("LESTRADE_DATABASE_URL", "AIRFLOW_CONN_LESTRADE_RDS", "DATABASE_URL"):
        v = (os.environ.get(key) or "").strip()
        if v:
            # Airflow URI sometimes uses postgresql+psycopg2://
            if v.startswith("postgresql+psycopg2://"):
                v = "postgresql://" + v[len("postgresql+psycopg2://") :]
            return v
    raise ValueError(
        "Set LESTRADE_DATABASE_URL or AIRFLOW_CONN_LESTRADE_RDS to your Supabase Postgres URI"
    )


def connect():
    return psycopg2.connect(database_url())


def list_trading_dates(conn: Any, *, limit: int = 30) -> list[date]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT trading_date
            FROM universe_snapshot
            ORDER BY trading_date DESC
            LIMIT %s
            """,
            (limit,),
        )
        return [r[0] for r in (cur.fetchall() or [])]


def tickers_for_session(
    conn: Any,
    trading_date: date,
    *,
    universe_group: str | None = None,
) -> list[tuple[str, str]]:
    """Return [(ticker, universe_group), ...] for a session."""
    with conn.cursor() as cur:
        if universe_group in ("high", "low"):
            cur.execute(
                """
                SELECT ticker, universe_group
                FROM universe_snapshot
                WHERE trading_date = %s AND universe_group = %s
                ORDER BY ticker
                """,
                (trading_date, universe_group),
            )
        else:
            cur.execute(
                """
                SELECT ticker, universe_group
                FROM universe_snapshot
                WHERE trading_date = %s
                ORDER BY universe_group, ticker
                """,
                (trading_date,),
            )
        return [(str(t), str(g)) for t, g in (cur.fetchall() or [])]
