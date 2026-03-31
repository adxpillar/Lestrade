from __future__ import annotations

import hashlib
from datetime import date, datetime, timezone
from typing import Any

from lestrade_ingest.db import connection_cursor

EMPTY_CONTENT_SHA256 = hashlib.sha256(b"").hexdigest()


def get_filing_raw_row(conn: Any, accession_number: str) -> dict[str, Any] | None:
    with connection_cursor(conn) as cur:
        cur.execute(
            """
            SELECT accession_number, cik, filing_date, accepted_at, primary_document,
                   raw_xml_s3_uri, raw_xml, content_sha256, fetched_at, http_status, fetch_error
            FROM filing_raw WHERE accession_number = %s
            """,
            (accession_number,),
        )
        row = cur.fetchone()
        if not row:
            return None
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))


def upsert_filing_raw_success(
    conn: Any,
    *,
    accession_number: str,
    cik: str,
    filing_date: date,
    accepted_at: datetime | None,
    primary_document: str,
    raw_xml_s3_uri: str | None,
    raw_xml: bytes | None,
    content_sha256: str,
    fetched_at: datetime,
    http_status: int,
) -> None:
    with connection_cursor(conn) as cur:
        cur.execute(
            """
            INSERT INTO filing_raw (
                accession_number, cik, filing_date, accepted_at, primary_document,
                raw_xml_s3_uri, raw_xml, content_sha256, fetched_at, http_status, fetch_error
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL)
            ON CONFLICT (accession_number) DO UPDATE SET
                cik = EXCLUDED.cik,
                filing_date = EXCLUDED.filing_date,
                accepted_at = COALESCE(EXCLUDED.accepted_at, filing_raw.accepted_at),
                primary_document = EXCLUDED.primary_document,
                raw_xml_s3_uri = EXCLUDED.raw_xml_s3_uri,
                raw_xml = EXCLUDED.raw_xml,
                content_sha256 = EXCLUDED.content_sha256,
                fetched_at = EXCLUDED.fetched_at,
                http_status = EXCLUDED.http_status,
                fetch_error = NULL
            """,
            (
                accession_number,
                cik,
                filing_date,
                accepted_at,
                primary_document,
                raw_xml_s3_uri,
                raw_xml,
                content_sha256,
                fetched_at,
                http_status,
            ),
        )


def record_filing_raw_fetch_failure(
    conn: Any,
    *,
    accession_number: str,
    cik: str,
    filing_date: date,
    primary_document: str,
    http_status: int,
    fetch_error: str,
    fetched_at: datetime | None = None,
) -> None:
    """Record a failed fetch without clobbering a previously successful artifact."""
    when = fetched_at or datetime.now(timezone.utc)
    existing = get_filing_raw_row(conn, accession_number)
    with connection_cursor(conn) as cur:
        if existing and existing.get("content_sha256") != EMPTY_CONTENT_SHA256:
            cur.execute(
                """
                UPDATE filing_raw
                SET http_status = %s, fetch_error = %s, fetched_at = %s
                WHERE accession_number = %s
                """,
                (http_status, fetch_error, when, accession_number),
            )
            return
        cur.execute(
            """
            INSERT INTO filing_raw (
                accession_number, cik, filing_date, accepted_at, primary_document,
                raw_xml_s3_uri, raw_xml, content_sha256, fetched_at, http_status, fetch_error
            ) VALUES (%s, %s, %s, NULL, %s, NULL, NULL, %s, %s, %s, %s)
            ON CONFLICT (accession_number) DO UPDATE SET
                http_status = EXCLUDED.http_status,
                fetch_error = EXCLUDED.fetch_error,
                fetched_at = EXCLUDED.fetched_at,
                cik = EXCLUDED.cik,
                filing_date = EXCLUDED.filing_date,
                primary_document = EXCLUDED.primary_document
            """,
            (
                accession_number,
                cik,
                filing_date,
                primary_document,
                EMPTY_CONTENT_SHA256,
                when,
                http_status,
                fetch_error,
            ),
        )
