"""Unit tests for yfinance profile field normalization (no network)."""

from decimal import Decimal

from lestrade_enrich.company_profile import (
    classify_yfinance_info,
    profile_fields_from_yfinance_info,
    ticker_to_yahoo_symbol,
)


def test_ticker_to_yahoo_symbol_class_share() -> None:
    assert ticker_to_yahoo_symbol("brk.b") == "BRK-B"
    assert ticker_to_yahoo_symbol("AAPL") == "AAPL"


def test_profile_fields_from_yfinance_info() -> None:
    info = {
        "sector": "Technology",
        "industry": "Consumer Electronics",
        "marketCap": 3_000_000_000_000,
        "industryKey": "consumer-electronics",
        "quoteType": "EQUITY",
        "longName": "Apple Inc.",
        "currency": "USD",
    }
    out = profile_fields_from_yfinance_info(info)
    assert out["sector"] == "Technology"
    assert out["industry"] == "Consumer Electronics"
    assert out["market_cap"] == Decimal("3000000000000")
    assert out["sic_code"] == "consumer-electronics"
    assert out["quote_type"] == "EQUITY"
    assert out["long_name"] == "Apple Inc."
    assert out["currency"] == "USD"


def test_classify_yfinance_info_ok_with_long_name() -> None:
    assert classify_yfinance_info({"longName": "X"}) == "ok"


def test_classify_yfinance_info_no_data() -> None:
    assert classify_yfinance_info({}) == "no_data"


def test_classify_yfinance_info_ok_with_market_cap() -> None:
    assert classify_yfinance_info({"marketCap": 100}) == "ok"
