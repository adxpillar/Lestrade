from __future__ import annotations

from datetime import date
from pathlib import Path

from lestrade_ingest.universe import (
    barchart_universe_filenames,
    barchart_universe_paths,
    parse_trading_date_from_barchart_filename,
    resolve_barchart_csv_pair,
    resolve_barchart_report_dir,
    tickers_from_barchart_csv,
)


def test_barchart_universe_filenames_mmddyyyy() -> None:
    d = date(2026, 5, 6)
    hi, lo = barchart_universe_filenames(d)
    assert hi == "all-us-exchanges-3-month-new-highs-05-06-2026.csv"
    assert lo == "all-us-exchanges-3-month-new-lows-05-06-2026.csv"


def test_barchart_universe_paths() -> None:
    d = date(2026, 5, 6)
    p = Path("/tmp/reports")
    hi, lo = barchart_universe_paths(p, d)
    assert hi == Path("/tmp/reports/all-us-exchanges-3-month-new-highs-05-06-2026.csv")
    assert lo == Path("/tmp/reports/all-us-exchanges-3-month-new-lows-05-06-2026.csv")


def test_tickers_from_barchart_csv_sample_highs() -> None:
    repo = Path(__file__).resolve().parent.parent
    path = repo / "barchart_report_today" / "all-us-exchanges-3-month-new-highs-05-06-2026.csv"
    tickers = tickers_from_barchart_csv(path)
    assert tickers[:3] == ["AAPL", "ABCB", "ABEO"]
    assert len(tickers) >= 400


def test_resolve_barchart_report_dir_explicit() -> None:
    p = Path("/tmp/lestrade_barchart_reports")
    assert resolve_barchart_report_dir(str(p)) == p.resolve()


def test_parse_trading_date_from_barchart_filename() -> None:
    assert parse_trading_date_from_barchart_filename(
        Path("all-us-exchanges-3-month-new-highs-05-06-2026.csv")
    ) == date(2026, 5, 6)
    assert parse_trading_date_from_barchart_filename(Path("not-a-barchart.csv")) is None


def test_resolve_barchart_csv_pair_matching(tmp_path: Path) -> None:
    (tmp_path / "all-us-exchanges-3-month-new-highs-05-06-2026.csv").write_text("Symbol\nX\n")
    (tmp_path / "all-us-exchanges-3-month-new-lows-05-06-2026.csv").write_text("Symbol\nY\n")
    d, hi, lo = resolve_barchart_csv_pair(tmp_path)
    assert d == date(2026, 5, 6)
    assert hi.name.startswith("all-us-exchanges-3-month-new-highs")
    assert lo.name.startswith("all-us-exchanges-3-month-new-lows")


def test_resolve_barchart_csv_pair_date_mismatch(tmp_path: Path) -> None:
    (tmp_path / "all-us-exchanges-3-month-new-highs-05-06-2026.csv").touch()
    (tmp_path / "all-us-exchanges-3-month-new-lows-05-07-2026.csv").touch()
    try:
        resolve_barchart_csv_pair(tmp_path)
        raise AssertionError("expected ValueError")
    except ValueError as e:
        assert "do not match" in str(e).lower() or "disagree" in str(e).lower()


def test_tickers_from_barchart_round_small_csv(tmp_path: Path) -> None:
    f = tmp_path / "tiny.csv"
    f.write_text("Symbol,Name\nZZZ,Zeta\nAAA,Alpha\nAAA,Alpha\n")
    tickers = tickers_from_barchart_csv(f)
    assert sorted(tickers) == ["AAA", "ZZZ"]
