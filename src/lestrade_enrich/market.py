from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from math import sqrt
from typing import Any, Iterable

from psycopg2.extras import execute_batch

from lestrade_enrich.stooq import sanitize_ohlc_for_numeric_18_6, sanitize_volume_for_bigint
from lestrade_ingest.db import connection_cursor
from lestrade_ingest.universe import normalize_ticker, tickers_for_date

_log = logging.getLogger(__name__)


@dataclass(frozen=True)
class MarketContextRow:
    trading_date: date
    universe_group: str
    ticker: str
    cik: str | None
    close: float | None
    volume: int | None
    ret_1d: float | None
    ret_5d: float | None
    ret_20d: float | None
    vol_20d: float | None
    source: str = "stooq"


def upsert_security_daily_prices(
    conn: Any,
    *,
    ticker: str,
    bars: Iterable[dict[str, Any]],
    source: str = "stooq",
) -> int:
    """
    Upsert price rows into `security_daily_prices`.

    bars items: {'price_date': date, 'open': float|None, 'high':..., 'low':..., 'close':..., 'volume': int|None}
    Returns number of rows upserted.
    """
    t = normalize_ticker(ticker)
    rows = list(bars)
    if not t or not rows:
        return 0
    sql = """
        INSERT INTO security_daily_prices
          (ticker, price_date, open, high, low, close, volume, source)
        VALUES
          (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker, price_date) DO UPDATE
          SET open = EXCLUDED.open,
              high = EXCLUDED.high,
              low = EXCLUDED.low,
              close = EXCLUDED.close,
              volume = EXCLUDED.volume,
              source = EXCLUDED.source,
              ingested_at = now()
    """
    argslist: list[tuple] = []
    skipped = 0
    for b in rows:
        close = sanitize_ohlc_for_numeric_18_6(b.get("close"))
        if close is None:
            skipped += 1
            continue
        argslist.append(
            (
                t,
                b["price_date"],
                sanitize_ohlc_for_numeric_18_6(b.get("open")),
                sanitize_ohlc_for_numeric_18_6(b.get("high")),
                sanitize_ohlc_for_numeric_18_6(b.get("low")),
                close,
                sanitize_volume_for_bigint(b.get("volume")),
                source,
            )
        )
    if skipped:
        _log.warning(
            "Skipped %s bar(s) for ticker %s: close missing or outside NUMERIC(18,6) range",
            skipped,
            t,
        )
    if not argslist:
        return 0
    with connection_cursor(conn) as cur:
        execute_batch(cur, sql, argslist, page_size=500)
        return len(argslist)


def _get_close_series(conn: Any, ticker: str, start: date, end: date) -> list[tuple[date, float, int | None]]:
    t = normalize_ticker(ticker)
    with connection_cursor(conn) as cur:
        cur.execute(
            """
            SELECT price_date, close, volume
            FROM security_daily_prices
            WHERE ticker = %s AND price_date BETWEEN %s AND %s AND close IS NOT NULL
            ORDER BY price_date
            """,
            (t, start, end),
        )
        rows = cur.fetchall() or []
    out: list[tuple[date, float, int | None]] = []
    for d, c, v in rows:
        try:
            out.append((d, float(c), int(v) if v is not None else None))
        except Exception:
            continue
    return out


def _return_over_days(series: list[tuple[date, float, int | None]], days_back: int) -> float | None:
    if len(series) < days_back + 1:
        return None
    cur = series[-1][1]
    prev = series[-(days_back + 1)][1]
    if prev == 0:
        return None
    return (cur / prev) - 1.0


def _volatility_20d(series: list[tuple[date, float, int | None]]) -> float | None:
    # Simple close-to-close vol over last 20 available bars (not annualized).
    if len(series) < 21:
        return None
    closes = [c for _, c, _ in series[-21:]]
    rets = []
    for i in range(1, len(closes)):
        if closes[i - 1] == 0:
            continue
        rets.append((closes[i] / closes[i - 1]) - 1.0)
    if len(rets) < 2:
        return None
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
    return sqrt(var)


def compute_universe_market_context(conn: Any, trading_date: date) -> list[MarketContextRow]:
    """
    For each universe ticker on `trading_date`, compute simple price-based features
    using cached `security_daily_prices`.
    """
    groups = tickers_for_date(conn, trading_date)
    tickers = sorted(set(groups["high"]) | set(groups["low"]))
    if not tickers:
        return []

    # Pull CIKs from universe_snapshot.
    cik_map: dict[str, str | None] = {}
    with connection_cursor(conn) as cur:
        cur.execute(
            """
            SELECT ticker, cik
            FROM universe_snapshot
            WHERE trading_date = %s
            """,
            (trading_date,),
        )
        for t, cik in cur.fetchall() or []:
            cik_map[normalize_ticker(t)] = (str(cik) if cik is not None else None)

    out: list[MarketContextRow] = []
    start = trading_date - timedelta(days=60)  # enough to cover 20 trading bars
    end = trading_date
    for t in tickers:
        series = _get_close_series(conn, t, start, end)
        close = series[-1][1] if series else None
        vol = series[-1][2] if series else None
        out.append(
            MarketContextRow(
                trading_date=trading_date,
                universe_group=("high" if t in set(groups["high"]) else "low"),
                ticker=t,
                cik=cik_map.get(normalize_ticker(t)),
                close=close,
                volume=vol,
                ret_1d=_return_over_days(series, 1),
                ret_5d=_return_over_days(series, 5),
                ret_20d=_return_over_days(series, 20),
                vol_20d=_volatility_20d(series),
            )
        )
    return out


def upsert_universe_market_context(conn: Any, rows: Iterable[MarketContextRow]) -> int:
    rs = list(rows)
    if not rs:
        return 0
    with connection_cursor(conn) as cur:
        n = 0
        for r in rs:
            cur.execute(
                """
                INSERT INTO universe_market_context
                  (trading_date, universe_group, ticker, cik, close, volume,
                   ret_1d, ret_5d, ret_20d, vol_20d, source)
                VALUES
                  (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (trading_date, universe_group, ticker) DO UPDATE
                  SET cik = EXCLUDED.cik,
                      close = EXCLUDED.close,
                      volume = EXCLUDED.volume,
                      ret_1d = EXCLUDED.ret_1d,
                      ret_5d = EXCLUDED.ret_5d,
                      ret_20d = EXCLUDED.ret_20d,
                      vol_20d = EXCLUDED.vol_20d,
                      source = EXCLUDED.source,
                      created_at = now()
                """,
                (
                    r.trading_date,
                    r.universe_group,
                    r.ticker,
                    r.cik,
                    r.close,
                    r.volume,
                    r.ret_1d,
                    r.ret_5d,
                    r.ret_20d,
                    r.vol_20d,
                    r.source,
                ),
            )
            n += 1
        return n

