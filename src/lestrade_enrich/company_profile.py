from __future__ import annotations

import logging
import time
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from lestrade_ingest.db import connection_cursor
from lestrade_ingest.universe import normalize_ticker

_log = logging.getLogger(__name__)


def ticker_to_yahoo_symbol(ticker: str) -> str:
    """Map universe ticker (e.g. BRK.B) to Yahoo-style symbol (BRK-B)."""
    u = normalize_ticker(ticker).upper()
    return u.replace(".", "-")


def profile_fields_from_yfinance_info(info: dict[str, Any]) -> dict[str, Any]:
    """
    Normalize a subset of yfinance ``Ticker.info`` into DB columns.
    Pure function for tests.
    """
    sector = info.get("sector")
    industry = info.get("industry")
    quote_type = info.get("quoteType")
    long_name = info.get("longName") or info.get("shortName")
    currency = info.get("currency")
    sic_code = info.get("industryKey") or info.get("industryDisp")

    mc = info.get("marketCap")
    market_cap: Decimal | None = None
    if mc is not None:
        try:
            market_cap = Decimal(str(int(mc)))
        except (ValueError, TypeError, InvalidOperation):
            market_cap = None

    out = {
        "sector": str(sector).strip() if sector else None,
        "industry": str(industry).strip() if industry else None,
        "market_cap": market_cap,
        "sic_code": str(sic_code).strip() if sic_code else None,
        "quote_type": str(quote_type).strip() if quote_type else None,
        "long_name": str(long_name).strip() if long_name else None,
        "currency": str(currency).strip() if currency else None,
    }
    return out


def classify_yfinance_info(info: dict[str, Any]) -> str:
    """Return ok | no_data for a non-empty yfinance info dict."""
    if not info:
        return "no_data"
    if info.get("quoteType") == "NONE" or str(info.get("symbol", "")).endswith("=X"):
        return "no_data"
    if long_name := (info.get("longName") or info.get("shortName")):
        if str(long_name).strip():
            return "ok"
    if info.get("marketCap") or info.get("regularMarketPrice") is not None:
        return "ok"
    return "no_data"


def fetch_yfinance_info(yahoo_symbol: str, *, timeout_s: float = 25.0) -> dict[str, Any]:
    """Blocking fetch of yfinance ``info`` dict (requires optional ``[enrich]`` extra)."""
    import yfinance as yf  # type: ignore[import-untyped]

    try:
        t = yf.Ticker(yahoo_symbol, timeout=int(timeout_s))
    except TypeError:
        t = yf.Ticker(yahoo_symbol)
    get_info = getattr(t, "get_info", None)
    raw: Any = get_info() if callable(get_info) else t.info
    return raw if isinstance(raw, dict) else {}


def tickers_with_cik_for_trading_date(conn: Any, trading_date: date) -> list[tuple[str, str | None]]:
    """Distinct tickers on a universe session with a representative CIK (if any)."""
    with connection_cursor(conn) as cur:
        cur.execute(
            """
            SELECT ticker, max(cik::text) AS cik
            FROM universe_snapshot
            WHERE trading_date = %s
            GROUP BY ticker
            ORDER BY ticker
            """,
            (trading_date,),
        )
        rows = cur.fetchall() or []
    out: list[tuple[str, str | None]] = []
    for t, cik in rows:
        nt = normalize_ticker(str(t))
        if cik is None:
            cik_s = None
        else:
            s = str(cik).strip()
            cik_s = s.zfill(10)[:10] if s else None
        out.append((nt, cik_s))
    return out


def upsert_universe_company_profile_row(
    conn: Any,
    *,
    trading_date: date,
    ticker: str,
    cik: str | None,
    fields: dict[str, Any],
    enrichment_status: str,
    error_message: str | None,
    source: str = "yfinance",
) -> None:
    now = datetime.now(timezone.utc)
    with connection_cursor(conn) as cur:
        cur.execute(
            """
            INSERT INTO universe_company_profile (
                trading_date, ticker, cik, sector, industry, market_cap, sic_code,
                quote_type, long_name, currency, enrichment_status, error_message,
                source, fetched_at, updated_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (trading_date, ticker) DO UPDATE SET
                cik = EXCLUDED.cik,
                sector = EXCLUDED.sector,
                industry = EXCLUDED.industry,
                market_cap = EXCLUDED.market_cap,
                sic_code = EXCLUDED.sic_code,
                quote_type = EXCLUDED.quote_type,
                long_name = EXCLUDED.long_name,
                currency = EXCLUDED.currency,
                enrichment_status = EXCLUDED.enrichment_status,
                error_message = EXCLUDED.error_message,
                source = EXCLUDED.source,
                fetched_at = EXCLUDED.fetched_at,
                updated_at = EXCLUDED.updated_at
            """,
            (
                trading_date,
                ticker,
                cik,
                fields.get("sector"),
                fields.get("industry"),
                fields.get("market_cap"),
                fields.get("sic_code"),
                fields.get("quote_type"),
                fields.get("long_name"),
                fields.get("currency"),
                enrichment_status,
                error_message,
                source,
                now,
                now,
            ),
        )


def enrich_universe_company_profiles_for_date(
    conn: Any,
    trading_date: date,
    *,
    sleep_s: float = 0.35,
    timeout_s: float = 25.0,
) -> tuple[int, int, int]:
    """
    For each ticker in ``universe_snapshot`` on ``trading_date``, fetch Yahoo Finance
    profile data and upsert ``universe_company_profile``.

    Returns: (ok_count, no_data_count, error_count)
    """
    pairs = tickers_with_cik_for_trading_date(conn, trading_date)
    ok_c = no_c = err_c = 0
    for i, (ticker, cik) in enumerate(pairs):
        ysym = ticker_to_yahoo_symbol(ticker)
        fields: dict[str, Any] = {
            "sector": None,
            "industry": None,
            "market_cap": None,
            "sic_code": None,
            "quote_type": None,
            "long_name": None,
            "currency": None,
        }
        status = "error"
        err: str | None = None
        try:
            info = fetch_yfinance_info(ysym, timeout_s=timeout_s)
            status = classify_yfinance_info(info)
            if status == "ok":
                fields = profile_fields_from_yfinance_info(info)
            else:
                fields = {**fields, **profile_fields_from_yfinance_info(info)}
        except Exception as e:
            status = "error"
            err = str(e)[:2000]
            _log.warning("yfinance profile failed for %s (%s): %s", ticker, ysym, e)
        upsert_universe_company_profile_row(
            conn,
            trading_date=trading_date,
            ticker=ticker,
            cik=cik,
            fields=fields,
            enrichment_status=status,
            error_message=err,
        )
        if status == "ok":
            ok_c += 1
        elif status == "no_data":
            no_c += 1
        else:
            err_c += 1
        if i + 1 < len(pairs) and sleep_s > 0:
            time.sleep(sleep_s)
    return ok_c, no_c, err_c
