from datetime import date

from lestrade_edgar.discovery import (
    daily_master_index_url,
    iter_form4_from_submissions,
    parse_filing_index_for_primary_xml,
    parse_master_idx_form4,
    submissions_json_url,
)
from lestrade_edgar.fetch import filing_index_url, primary_document_url


def test_submissions_json_url():
    assert submissions_json_url("0000320193") == (
        "https://data.sec.gov/submissions/CIK0000320193.json"
    )


def test_daily_master_index_url():
    assert daily_master_index_url(date(2024, 3, 15)) == (
        "https://www.sec.gov/Archives/edgar/daily-index/2024/QTR1/master20240315.idx"
    )
    assert daily_master_index_url(date(2024, 5, 1)).endswith("/QTR2/master20240501.idx")


def test_primary_document_url():
    u = primary_document_url("0000320193", "0000320193-25-000042", "xslF345X05/doc.xml")
    assert u == (
        "https://www.sec.gov/Archives/edgar/data/320193/"
        "000032019325000042/xslF345X05/doc.xml"
    )


def test_filing_index_url():
    u = filing_index_url("0000320193", "0000320193-25-000042")
    assert u.endswith("/0000320193-25-000042-index.htm")
    assert "/320193/000032019325000042/" in u


def test_iter_form4_from_submissions():
    payload = {
        "cik": "320193",
        "filings": {
            "recent": {
                "form": ["4", "10-K", "4/A"],
                "filingDate": ["2025-01-10", "2025-01-11", "2025-01-12"],
                "accessionNumber": ["0000320193-25-000001", "x-2", "0000320193-25-000003"],
                "primaryDocument": ["a.xml", "b.htm", "c.xml"],
            }
        },
    }
    rows = list(iter_form4_from_submissions(payload))
    assert len(rows) == 2
    assert rows[0].form == "4"
    assert rows[0].cik == "0000320193"
    assert rows[0].accession_number == "0000320193-25-000001"
    assert rows[1].form == "4/A"


def test_parse_filing_index_picks_form4_xml():
    html = '<a href="/Archives/edgar/data/1/x/xslf345x05/wf-form4_1.xml">4</a>'
    assert parse_filing_index_for_primary_xml(html).endswith("wf-form4_1.xml")


def test_parse_master_idx_pipe():
    text = """320193|Apple Inc.|4|2025-01-10|edgar/data/320193/0000320193-25-000001/x.xml
9999999|Other|10-K|2025-01-10|edgar/data/1/x.htm
"""
    rows = parse_master_idx_form4(text)
    assert len(rows) == 1
    assert rows[0].cik == "0000320193"
    assert rows[0].form_type == "4"
    assert rows[0].date_filed == date(2025, 1, 10)
