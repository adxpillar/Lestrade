from __future__ import annotations

from datetime import date
from typing import Any

from lestrade_ingest.db import connection_cursor
def _normalize_ticker(t: str) -> str:
    return (t or "").strip().upper()


def load_universe_group_by_ticker(
    conn: Any,
    trading_dates: list[date],
) -> dict[tuple[date, str], str | None]:
    """
    Option 3: return universe_group per (trading_date, ticker), or None if
    ticker appears as both high and low on that date.
    """
    if not trading_dates:
        return {}
    with connection_cursor(conn) as cur:
        cur.execute(
            """
            SELECT trading_date, ticker, array_agg(universe_group ORDER BY universe_group) AS groups
            FROM universe_snapshot
            WHERE trading_date = ANY(%s)
            GROUP BY trading_date, ticker
            """,
            (trading_dates,),
        )
        rows = cur.fetchall() or []
    out: dict[tuple[date, str], str | None] = {}
    for td, ticker, groups in rows:
        nt = _normalize_ticker(str(ticker))
        gs = list(groups or [])
        if len(gs) == 1:
            out[(td, nt)] = str(gs[0])
        else:
            out[(td, nt)] = None
    return out
