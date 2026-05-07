from __future__ import annotations

import csv
import logging
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Literal


UniverseGroup = Literal["high", "low"]


@dataclass(frozen=True)
class UniverseRow:
    trading_date: date
    ticker: str
    universe_group: UniverseGroup
    cik: str | None
    source: str = "manual_env"


def normalize_ticker(t: str) -> str:
    return (t or "").strip().upper()


_RE_TICKER = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")


def is_probable_ticker_symbol(sym: str) -> bool:
    """
    Conservative validation for US ticker symbols.

    Filters out common non-symbol tokens that can appear in CSV exports
    (timestamps, dates, blank values, etc.).
    """
    s = normalize_ticker(sym)
    if not s:
        return False
    if ":" in s or " " in s or "/" in s:
        return False
    if s[0].isdigit():
        return False
    return _RE_TICKER.fullmatch(s) is not None


def barchart_universe_filenames(trading_date: date) -> tuple[str, str]:
    """
    Expected Barchart export filenames embedded date suffix (MM-DD-YYYY).

    Example for ``2026-05-06``::
        all-us-exchanges-3-month-new-highs-05-06-2026.csv
    """
    stamp = trading_date.strftime("%m-%d-%Y")
    return (
        f"all-us-exchanges-3-month-new-highs-{stamp}.csv",
        f"all-us-exchanges-3-month-new-lows-{stamp}.csv",
    )


def resolve_barchart_report_dir(dir_raw: str | None) -> Path:
    """
    Prefer ``LESTRADE_BARCHART_DIR`` when set.

    Else use ``/opt/airflow/lestrade/barchart_report_today`` when the repo volume
    is mounted there (Compose), otherwise ``./barchart_report_today`` under cwd.
    """
    s = (dir_raw or "").strip()
    if s:
        return Path(s).expanduser().resolve()
    docker_default = Path("/opt/airflow/lestrade/barchart_report_today")
    if docker_default.parent.is_dir():
        return docker_default
    return (Path.cwd() / "barchart_report_today").resolve()


def resolve_barchart_archive_dir(dir_raw: str | None) -> Path:
    """
    Prefer ``LESTRADE_BARCHART_ARCHIVE_DIR`` when set.

    Else use ``/opt/airflow/lestrade/barchart_report_archive`` when the repo volume
    is mounted there (Compose), otherwise ``./barchart_report_archive`` under cwd.
    """
    s = (dir_raw or "").strip()
    if s:
        return Path(s).expanduser().resolve()
    docker_default = Path("/opt/airflow/lestrade/barchart_report_archive")
    if docker_default.parent.is_dir():
        return docker_default
    return (Path.cwd() / "barchart_report_archive").resolve()


def barchart_universe_paths(report_dir: Path, trading_date: date) -> tuple[Path, Path]:
    hi, lo = barchart_universe_filenames(trading_date)
    return (report_dir / hi, report_dir / lo)


_RE_BARCHART_HIGH = re.compile(
    r"^all-us-exchanges-3-month-new-highs-(\d{2})-(\d{2})-(\d{4})\.csv$",
    re.IGNORECASE,
)
_RE_BARCHART_LOW = re.compile(
    r"^all-us-exchanges-3-month-new-lows-(\d{2})-(\d{2})-(\d{4})\.csv$",
    re.IGNORECASE,
)


def _date_from_barchart_suffix(match: re.Match[str]) -> date | None:
    mm_s, dd_s, yyyy_s = match.groups()
    try:
        return date(int(yyyy_s), int(mm_s), int(dd_s))
    except ValueError:
        return None


def parse_trading_date_from_barchart_filename(path: Path) -> date | None:
    """
    Return the calendar date embedded in a Barchart highs/lows export filename
    (``MM-DD-YYYY`` before ``.csv``), or None if the name does not match.
    """
    name = path.name
    m = _RE_BARCHART_HIGH.match(name) or _RE_BARCHART_LOW.match(name)
    if not m:
        return None
    return _date_from_barchart_suffix(m)


def resolve_barchart_csv_pair(report_dir: Path) -> tuple[date, Path, Path]:
    """
    Locate exactly one highs and one lows CSV under ``report_dir``; parse ``MM-DD-YYYY``
    from each name and ensure both refer to the same session date.

    Returns ``(trading_date, high_path, low_path)``.
    """
    rd = report_dir.resolve()
    if not rd.is_dir():
        raise FileNotFoundError(f"Barchart report directory does not exist: {rd}")

    highs = sorted(rd.glob("all-us-exchanges-3-month-new-highs-*.csv"))
    lows = sorted(rd.glob("all-us-exchanges-3-month-new-lows-*.csv"))

    if len(highs) != 1:
        raise FileNotFoundError(
            f"Expected exactly one highs CSV in {rd}, found {len(highs)} "
            f"({', '.join(p.name for p in highs) or 'none'})."
        )
    if len(lows) != 1:
        raise FileNotFoundError(
            f"Expected exactly one lows CSV in {rd}, found {len(lows)} "
            f"({', '.join(p.name for p in lows) or 'none'})."
        )

    hi_path, lo_path = highs[0], lows[0]
    d_hi = parse_trading_date_from_barchart_filename(hi_path)
    d_lo = parse_trading_date_from_barchart_filename(lo_path)
    if d_hi is None:
        raise ValueError(f"Cannot parse MM-DD-YYYY from highs filename: {hi_path.name!r}")
    if d_lo is None:
        raise ValueError(f"Cannot parse MM-DD-YYYY from lows filename: {lo_path.name!r}")
    if d_hi != d_lo:
        raise ValueError(
            f"Barchart CSV filename dates do not match: highs → {d_hi.isoformat()} ({hi_path.name}), "
            f"lows → {d_lo.isoformat()} ({lo_path.name})."
        )
    return d_hi, hi_path, lo_path


def tickers_from_barchart_csv(path: Path) -> list[str]:
    """Read ticker symbols from a Barchart-style export (expects a ``Symbol`` column)."""
    if not path.is_file():
        raise FileNotFoundError(f"Barchart universe CSV not found: {path}")
    log = logging.getLogger(__name__)
    symbols: list[str] = []
    ignored = 0
    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError(f"CSV has no header row: {path}")
        key: str | None = None
        for cand in ("Symbol", "symbol"):
            if cand in reader.fieldnames:
                key = cand
                break
        if key is None:
            raise ValueError(
                f"CSV {path} missing required Symbol column; headers={reader.fieldnames!r}"
            )
        for row in reader:
            # Barchart exports sometimes append a one-line footer like:
            # "Downloaded from Barchart.com as of 05-06-2026 07:29pm CDT"
            # Treat that as an explicit end-of-file marker.
            row_text = " ".join(str(v) for v in (row or {}).values() if v is not None)
            if "downloaded from barchart.com" in row_text.lower():
                break
            cell = (row.get(key) or "").strip()
            if not cell:
                continue
            if not is_probable_ticker_symbol(cell):
                ignored += 1
                continue
            symbols.append(normalize_ticker(cell))
    if ignored:
        log.warning("Ignored %s non-ticker symbols from %s.", ignored, path.name)
    if not symbols:
        raise ValueError(f"No valid ticker symbols parsed from {path}")
    return parse_ticker_list("\n".join(symbols))


def parse_ticker_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    # Accept comma-separated and/or whitespace/newline separated.
    s = raw.replace("\n", ",").replace("\t", ",").replace(" ", ",")
    parts = [p.strip() for p in s.split(",") if p.strip()]
    out: list[str] = []
    seen: set[str] = set()
    for p in parts:
        n = normalize_ticker(p)
        if not n or n in seen:
            continue
        seen.add(n)
        out.append(n)
    return out


def _pad_cik(cik: str | int | None) -> str | None:
    if cik is None:
        return None
    s = str(cik).strip()
    digits = "".join(c for c in s if c.isdigit())
    if not digits:
        return None
    return digits.zfill(10)[:10]


def upsert_security_master(conn: Any, rows: dict[str, str | None]) -> None:
    """
    Upsert ticker→CIK mappings into `security_master`.

    `rows` maps normalized ticker -> CIK (10-digit) or None.
    """
    from lestrade_ingest.db import connection_cursor

    if not rows:
        return
    with connection_cursor(conn) as cur:
        for ticker, cik in rows.items():
            cur.execute(
                """
                INSERT INTO security_master (ticker, cik)
                VALUES (%s, %s)
                ON CONFLICT (ticker) DO UPDATE
                SET cik = EXCLUDED.cik,
                    updated_at = now()
                """,
                (ticker, cik),
            )


def upsert_universe_snapshot(conn: Any, universe_rows: Iterable[UniverseRow]) -> None:
    from lestrade_ingest.db import connection_cursor

    rows = list(universe_rows)
    if not rows:
        return
    with connection_cursor(conn) as cur:
        for r in rows:
            cur.execute(
                """
                INSERT INTO universe_snapshot (trading_date, ticker, universe_group, cik, source)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (trading_date, ticker, universe_group) DO UPDATE
                SET cik = EXCLUDED.cik,
                    source = EXCLUDED.source
                """,
                (r.trading_date, r.ticker, r.universe_group, r.cik, r.source),
            )


def latest_trading_date(conn: Any) -> date | None:
    from lestrade_ingest.db import connection_cursor

    with connection_cursor(conn) as cur:
        cur.execute("SELECT max(trading_date) FROM universe_snapshot")
        row = cur.fetchone()
        return row[0] if row and row[0] is not None else None


def universe_snapshot_exists_for_date(conn: Any, trading_date: date) -> bool:
    """True if `universe_snapshot` has at least one row for this trading date."""
    from lestrade_ingest.db import connection_cursor

    with connection_cursor(conn) as cur:
        cur.execute(
            "SELECT 1 FROM universe_snapshot WHERE trading_date = %s LIMIT 1",
            (trading_date,),
        )
        return cur.fetchone() is not None


def universe_ciks_for_date(conn: Any, trading_date: date) -> dict[UniverseGroup, list[str]]:
    """
    Returns {'high': [...], 'low': [...]} of CIKs for that date.
    """
    from lestrade_ingest.db import connection_cursor

    out: dict[UniverseGroup, list[str]] = {"high": [], "low": []}
    with connection_cursor(conn) as cur:
        cur.execute(
            """
            SELECT universe_group, cik
            FROM universe_snapshot
            WHERE trading_date = %s AND cik IS NOT NULL
            ORDER BY universe_group, cik
            """,
            (trading_date,),
        )
        for g, cik in cur.fetchall() or []:
            gg = str(g)
            if gg not in ("high", "low"):
                continue
            c = _pad_cik(cik)
            if c:
                out[gg].append(c)  # type: ignore[index]
    # De-dupe within each list
    out["high"] = sorted(set(out["high"]))
    out["low"] = sorted(set(out["low"]))
    return out


def ciks_for_date_and_tickers(conn: Any, trading_date: date, tickers: Iterable[str]) -> list[str]:
    """
    Return distinct CIKs for the given tickers on a trading_date.
    """
    from lestrade_ingest.db import connection_cursor

    ts = sorted({normalize_ticker(t) for t in tickers if normalize_ticker(t)})
    if not ts:
        return []
    with connection_cursor(conn) as cur:
        cur.execute(
            """
            SELECT DISTINCT cik
            FROM universe_snapshot
            WHERE trading_date = %s
              AND ticker = ANY(%s)
              AND cik IS NOT NULL
            ORDER BY cik
            """,
            (trading_date, ts),
        )
        rows = cur.fetchall() or []
    out = []
    for (cik,) in rows:
        c = _pad_cik(cik)
        if c:
            out.append(c)
    return sorted(set(out))


def tickers_for_date(conn: Any, trading_date: date) -> dict[UniverseGroup, list[str]]:
    """
    Returns {'high': [...], 'low': [...]} tickers for that date.
    """
    from lestrade_ingest.db import connection_cursor

    out: dict[UniverseGroup, list[str]] = {"high": [], "low": []}
    with connection_cursor(conn) as cur:
        cur.execute(
            """
            SELECT universe_group, ticker
            FROM universe_snapshot
            WHERE trading_date = %s
            ORDER BY universe_group, ticker
            """,
            (trading_date,),
        )
        for g, t in cur.fetchall() or []:
            gg = str(g)
            if gg not in ("high", "low"):
                continue
            tt = normalize_ticker(t)
            if tt:
                out[gg].append(tt)  # type: ignore[index]
    out["high"] = sorted(set(out["high"]))
    out["low"] = sorted(set(out["low"]))
    return out


def new_entrant_tickers(conn: Any, trading_date: date) -> dict[UniverseGroup, list[str]]:
    """
    Compare `trading_date` to previous available trading_date and return tickers newly present.
    """
    from lestrade_ingest.db import connection_cursor

    with connection_cursor(conn) as cur:
        cur.execute(
            """
            WITH dates AS (
              SELECT DISTINCT trading_date
              FROM universe_snapshot
              WHERE trading_date <= %s
              ORDER BY trading_date DESC
              LIMIT 2
            )
            SELECT array_agg(trading_date ORDER BY trading_date DESC) FROM dates
            """,
            (trading_date,),
        )
        row = cur.fetchone()
        dates = (row[0] if row else None) or []
    if len(dates) < 2:
        return {"high": [], "low": []}
    current, prev = dates[0], dates[1]

    cur_set = tickers_for_date(conn, current)
    prev_set = tickers_for_date(conn, prev)
    return {
        "high": sorted(set(cur_set["high"]) - set(prev_set["high"])),
        "low": sorted(set(cur_set["low"]) - set(prev_set["low"])),
    }

