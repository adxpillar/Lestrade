from __future__ import annotations

from lestrade_rag.citations import accession_to_edgar_index_url
from lestrade_rag.retrieve import _build_where


def test_edgar_url_with_cik() -> None:
    url = accession_to_edgar_index_url("0001234567-26-000001", "0000320193")
    assert url is not None
    assert "320193" in url
    assert "000123456726000001" in url
    assert url.endswith("-index.htm")


def test_build_where_ticker_and_side() -> None:
    w = _build_where(ticker="aapl", acquired_disposed="P", universe_group="high")
    assert w is not None
    assert "$and" in w
    assert {"issuer_ticker": "AAPL"} in w["$and"]
    assert {"acquired_disposed_code": "A"} in w["$and"]
    assert {"universe_group": "high"} in w["$and"]
