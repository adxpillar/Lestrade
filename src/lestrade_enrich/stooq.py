from __future__ import annotations

import csv
import logging
import math
from dataclasses import dataclass
from datetime import date
from io import StringIO
from time import sleep
from typing import Iterable

import requests

_log = logging.getLogger(__name__)

# Postgres NUMERIC(18,6): absolute value must round to strictly less than 10^12.
_PG_NUMERIC_18_6_ABS_MAX = 10**12
_PG_BIGINT_MAX = 9223372036854775807
_PG_BIGINT_MIN = -9223372036854775808


def sanitize_ohlc_for_numeric_18_6(x: float | None) -> float | None:
    """Coerce Stooq OHLC to values Postgres NUMERIC(18,6) accepts; None if unusable."""
    if x is None:
        return None
    try:
        xf = float(x)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(xf) or abs(xf) >= _PG_NUMERIC_18_6_ABS_MAX:
        return None
    return xf


def sanitize_volume_for_bigint(x: int | None) -> int | None:
    if x is None:
        return None
    try:
        xi = int(x)
    except (TypeError, ValueError):
        return None
    if xi > _PG_BIGINT_MAX or xi < _PG_BIGINT_MIN:
        return None
    return xi


@dataclass(frozen=True)
class DailyBar:
    price_date: date
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: int | None


def _to_float(s: str | None) -> float | None:
    try:
        ss = (s or "").strip()
        return float(ss) if ss else None
    except Exception:
        return None


def _to_int(s: str | None) -> int | None:
    try:
        ss = (s or "").strip()
        return int(float(ss)) if ss else None
    except Exception:
        return None


def stooq_symbol_for_ticker(ticker: str) -> str:
    """
    Stooq uses lower-case symbols, typically with `.us` for US equities.
    """
    t = (ticker or "").strip().lower()
    if not t:
        raise ValueError("ticker is required")
    if t.endswith(".us"):
        return t
    return f"{t}.us"


def fetch_daily_bars_stooq(
    ticker: str,
    *,
    api_key: str | None = None,
    session: requests.Session | None = None,
    timeout_s: float = 60.0,
) -> list[DailyBar]:
    """
    Fetch full available daily history for a ticker from Stooq.

    URL shape:
      https://stooq.com/q/d/l/?s=aapl.us&i=d
    """
    sym = stooq_symbol_for_ticker(ticker)
    api_key = (api_key or "").strip() or None
    url = f"https://stooq.com/q/d/l/?s={sym}&i=d"
    if api_key:
        url = f"{url}&apikey={api_key}"
    sess = session or requests.Session()
    # Stooq sometimes blocks/returns HTML unless we look like a browser.
    headers = {
        "User-Agent": "Mozilla/5.0 (Lestrade; enrichment; +https://github.com/)",
        "Accept": "text/csv, text/plain;q=0.9, */*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    last_err: Exception | None = None
    for attempt in range(3):
        try:
            resp = sess.get(url, timeout=timeout_s, headers=headers)
            resp.raise_for_status()
            text = resp.text or ""
            break
        except Exception as e:
            last_err = e
            if attempt >= 2:
                raise
            sleep(1.5 * (2**attempt))
    else:
        raise last_err or ValueError("Stooq fetch failed")

    head = (text or "").lstrip()[:200].lower()
    if head.startswith("<!doctype") or head.startswith("<html") or "<html" in head:
        snippet = (text or "").lstrip()[:200].replace("\n", " ")
        lower = snippet.lower()
        if "get your apikey" in lower or "get_apikey" in lower or "captcha" in lower:
            raise ValueError(
                "Stooq requires an API key (and sometimes a CAPTCHA). "
                "Get a key via `https://stooq.com/q/d/?s=<symbol>&get_apikey`, then set "
                "`LESTRADE_STOOQ_APIKEY` (or Airflow Variable `lestrade_stooq_apikey`). "
                f"Snippet: {snippet!r}"
            )
        raise ValueError(f"Unexpected Stooq HTML response for {ticker} ({sym}): {snippet!r}")

    if "Exceeded the daily hits limit" in text:
        raise ValueError("Stooq daily hits limit exceeded")

    if "Date,Open,High,Low,Close,Volume" not in text:
        snippet = (text or "").lstrip()[:200].replace("\n", " ")
        raise ValueError(f"Unexpected Stooq response for {ticker} ({sym}): {snippet!r}")

    reader = csv.DictReader(StringIO(text))
    out: list[DailyBar] = []
    partial_sanitize_logs = 0
    dropped_bad_close = 0
    first_bad_close_date: date | None = None
    last_bad_close_date: date | None = None
    sample_bad_close: float | None = None
    for row in reader:
        ds = (row.get("Date") or "").strip()
        if not ds:
            continue
        try:
            d = date.fromisoformat(ds)
        except ValueError:
            continue
        raw_o = _to_float(row.get("Open"))
        raw_h = _to_float(row.get("High"))
        raw_l = _to_float(row.get("Low"))
        raw_c = _to_float(row.get("Close"))
        raw_v = _to_int(row.get("Volume"))
        o = sanitize_ohlc_for_numeric_18_6(raw_o)
        h = sanitize_ohlc_for_numeric_18_6(raw_h)
        l_ = sanitize_ohlc_for_numeric_18_6(raw_l)
        c = sanitize_ohlc_for_numeric_18_6(raw_c)
        v = sanitize_volume_for_bigint(raw_v)
        if raw_c is not None and c is None:
            dropped_bad_close += 1
            if first_bad_close_date is None:
                first_bad_close_date = d
                sample_bad_close = raw_c
            last_bad_close_date = d
            continue
        if c is None:
            continue
        if any(
            raw is not None and san is None
            for raw, san in ((raw_o, o), (raw_h, h), (raw_l, l_), (raw_v, v))
        ):
            if partial_sanitize_logs < 5:
                partial_sanitize_logs += 1
                _log.debug(
                    "Sanitized out-of-range Stooq field(s) for %s on %s (open=%r high=%r low=%r vol=%r)",
                    sym,
                    d.isoformat(),
                    raw_o,
                    raw_h,
                    raw_l,
                    raw_v,
                )
        out.append(
            DailyBar(
                price_date=d,
                open=o,
                high=h,
                low=l_,
                close=c,
                volume=v,
            )
        )
    if dropped_bad_close:
        fd = first_bad_close_date.isoformat() if first_bad_close_date else "?"
        ld = last_bad_close_date.isoformat() if last_bad_close_date else "?"
        _log.warning(
            "Dropped %s Stooq bar(s) for %s: close not storable as NUMERIC(18,6) "
            "(often bad vendor data for reverse-split symbols; sample raw=%r, span %s..%s); "
            "kept %s usable bar(s)",
            dropped_bad_close,
            sym,
            sample_bad_close,
            fd,
            ld,
            len(out),
        )
    return out


def bars_in_range(bars: Iterable[DailyBar], start: date, end: date) -> list[DailyBar]:
    return [b for b in bars if start <= b.price_date <= end]

