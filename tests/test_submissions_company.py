"""SEC company submissions JSON → issuer metadata (pure)."""

from lestrade_edgar.submissions_company import issuer_metadata_from_submissions


def test_issuer_metadata_from_submissions_apple_like() -> None:
    payload = {
        "cik": 320193,
        "entityType": "operating",
        "sic": 3571,
        "sicDescription": "Electronic Computers",
        "name": "Apple Inc.",
        "tickers": ["AAPL"],
        "exchanges": ["Nasdaq"],
        "stateOfIncorporation": "CA",
        "fiscalYearEnd": "0927",
    }
    m = issuer_metadata_from_submissions(payload)
    assert m["entity_name"] == "Apple Inc."
    assert m["entity_type"] == "operating"
    assert m["sic_code"] == "3571"
    assert m["sic_description"] == "Electronic Computers"
    assert m["state_of_incorporation"] == "CA"
    assert m["fiscal_year_end"] == "0927"
    assert m["tickers"] == ["AAPL"]
    assert m["exchanges"] == ["Nasdaq"]


def test_issuer_metadata_empty_sic() -> None:
    m = issuer_metadata_from_submissions({"name": "X", "cik": 1})
    assert m["sic_code"] is None
    assert m["entity_name"] == "X"
