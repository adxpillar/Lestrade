from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Literal


@dataclass(frozen=True)
class Form4FilingRef:
    """A Form 4 or 4/A filing reference from submissions JSON or equivalent."""

    cik: str
    """Issuer CIK as 10-digit zero-padded string."""

    accession_number: str
    filing_date: date
    primary_document: str
    form: Literal["4", "4/A"]


@dataclass(frozen=True)
class MasterIndexRow:
    """One row from a daily (or full) EDGAR master index."""

    cik: str
    """CIK as 10-digit zero-padded string."""

    company_name: str
    form_type: str
    date_filed: date
    archive_path: str
    """Path under Archives, e.g. `edgar/data/320193/0000320193-25-000042/...`."""


@dataclass(frozen=True)
class FetchResult:
    """Result of downloading the primary document bytes from EDGAR."""

    content: bytes
    content_sha256_hex: str
    http_status: int
    url: str
    fetched_at: datetime


@dataclass(frozen=True)
class StoredRawXml:
    """Where raw XML landed, per DATA_CONTRACT (S3 URI or BYTEA, not both)."""

    content_sha256_hex: str
    http_status: int
    fetched_at: datetime
    raw_xml_s3_uri: str | None
    raw_xml: bytes | None
