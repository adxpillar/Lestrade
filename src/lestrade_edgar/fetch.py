from __future__ import annotations

import hashlib
from datetime import datetime, timezone

from lestrade_edgar.cik import accession_nodash, cik_path_segment, pad_cik
from lestrade_edgar.models import FetchResult


def filing_index_url(cik: str | int, accession_number: str) -> str:
    """URL of the filing's ``*-index.htm`` directory page on ``www.sec.gov``."""
    cik_p = pad_cik(cik)
    seg = cik_path_segment(cik_p)
    nodash = accession_nodash(accession_number)
    return (
        f"https://www.sec.gov/Archives/edgar/data/{seg}/{nodash}/"
        f"{accession_number}-index.htm"
    )


def primary_document_url(cik: str | int, accession_number: str, primary_document: str) -> str:
    """
    URL for the primary filing document on www.sec.gov.

    Path uses CIK without leading zeros (SEC convention).
    """
    cik_p = pad_cik(cik)
    seg = cik_path_segment(cik_p)
    nodash = accession_nodash(accession_number)
    doc = primary_document.lstrip("/")
    return f"https://www.sec.gov/Archives/edgar/data/{seg}/{nodash}/{doc}"


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def build_fetch_result(
    content: bytes,
    *,
    http_status: int,
    url: str,
    fetched_at: datetime | None = None,
) -> FetchResult:
    when = fetched_at or datetime.now(timezone.utc)
    return FetchResult(
        content=content,
        content_sha256_hex=sha256_hex(content),
        http_status=http_status,
        url=url,
        fetched_at=when,
    )
