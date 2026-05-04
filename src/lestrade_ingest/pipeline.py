from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Sequence
from zoneinfo import ZoneInfo

from lestrade_edgar.client import EdgarClient
from lestrade_edgar.exceptions import EdgarRequestError
from lestrade_edgar.models import Form4FilingRef, MasterIndexRow
from lestrade_edgar.storage import store_raw_xml, suggested_s3_key

from lestrade_ingest.config import IngestConfig
from lestrade_ingest.errors import record_ingestion_error
from lestrade_ingest.filing_raw import (
    EMPTY_CONTENT_SHA256,
    get_filing_raw_row,
    record_filing_raw_fetch_failure,
    upsert_filing_raw_success,
)
from lestrade_ingest.form4_parser import parse_form4_xml, parsed_at_now
from lestrade_ingest.loader import upsert_filing_and_transactions
from lestrade_ingest.paths import accession_from_archive_path, primary_document_from_archive_path


def _filing_parsed_clean(conn: Any, accession_number: str) -> bool:
    from lestrade_ingest.db import connection_cursor

    with connection_cursor(conn) as cur:
        cur.execute(
            "SELECT parse_error FROM filing WHERE accession_number = %s",
            (accession_number,),
        )
        row = cur.fetchone()
        return row is not None and row[0] is None


def _load_stored_xml_bytes(
    conn: Any,
    accession_number: str,
    config: IngestConfig,
    existing: dict[str, Any],
) -> bytes | None:
    if config.raw_storage == "bytea":
        b = existing.get("raw_xml")
        return bytes(b) if b else None
    uri = existing.get("raw_xml_s3_uri")
    if not uri or not uri.startswith("s3://") or config.s3_client is None:
        return None
    rest = uri[5:]
    if "/" not in rest:
        return None
    bucket, key = rest.split("/", 1)
    get = getattr(config.s3_client, "get_object", None)
    if get is None:
        return None
    obj = get(Bucket=bucket, Key=key)
    body = obj.get("Body")
    return body.read() if body is not None else None


def ingest_form4_ref(
    conn: Any,
    client: EdgarClient,
    ref: Form4FilingRef,
    config: IngestConfig,
    *,
    accepted_at: datetime | None = None,
) -> str:
    """
    Fetch (or reuse stored XML), persist ``filing_raw``, then parse into ``filing`` / ``form4_transaction``.

    Raw bytes are committed before parse so a parse failure still leaves a durable artifact.
    Returns: ``ingested``, ``skipped``, ``fetch_failed``, ``parse_failed``.
    """
    acc = ref.accession_number
    existing = get_filing_raw_row(conn, acc)

    skip_fetch = (
        not config.force_refetch
        and existing is not None
        and existing.get("content_sha256") not in (None, EMPTY_CONTENT_SHA256)
        and (
            (config.raw_storage == "bytea" and existing.get("raw_xml"))
            or (config.raw_storage == "s3" and existing.get("raw_xml_s3_uri"))
        )
    )

    xml_bytes: bytes | None = None

    if skip_fetch and existing is not None:
        xml_bytes = _load_stored_xml_bytes(conn, acc, config, existing)
        if xml_bytes is None:
            skip_fetch = False

    if not skip_fetch:
        try:
            fr = client.fetch_primary_document(ref.cik, acc, ref.primary_document)
        except EdgarRequestError as e:
            record_filing_raw_fetch_failure(
                conn,
                accession_number=acc,
                cik=ref.cik,
                filing_date=ref.filing_date,
                primary_document=ref.primary_document,
                http_status=e.status_code or 0,
                fetch_error=str(e),
            )
            record_ingestion_error(
                conn,
                stage="fetch",
                error_type="edgar_request_error",
                message=str(e),
                accession_number=acc,
                payload={"url": e.url},
            )
            conn.commit()
            return "fetch_failed"

        if fr.http_status != 200:
            record_filing_raw_fetch_failure(
                conn,
                accession_number=acc,
                cik=ref.cik,
                filing_date=ref.filing_date,
                primary_document=ref.primary_document,
                http_status=fr.http_status,
                fetch_error=f"HTTP {fr.http_status}",
            )
            record_ingestion_error(
                conn,
                stage="fetch",
                error_type="http_error",
                message=f"HTTP {fr.http_status}",
                accession_number=acc,
                payload={"url": fr.url},
            )
            conn.commit()
            return "fetch_failed"

        raw_uri: str | None = None
        raw_blob: bytes | None = None
        if config.raw_storage == "bytea":
            st = store_raw_xml(fr, mode="bytea")
            raw_blob = st.raw_xml
        else:
            if not config.s3_bucket or config.s3_client is None:
                raise ValueError('raw_storage="s3" requires s3_bucket and s3_client')
            key = suggested_s3_key(
                cik_padded=ref.cik,
                accession_number=acc,
                primary_document=ref.primary_document,
                content_sha256_hex=fr.content_sha256_hex,
            )
            st = store_raw_xml(
                fr,
                mode="s3",
                s3_client=config.s3_client,
                bucket=config.s3_bucket,
                key=key,
            )
            raw_uri = st.raw_xml_s3_uri

        upsert_filing_raw_success(
            conn,
            accession_number=acc,
            cik=ref.cik,
            filing_date=ref.filing_date,
            accepted_at=accepted_at,
            primary_document=ref.primary_document,
            raw_xml_s3_uri=raw_uri,
            raw_xml=raw_blob,
            content_sha256=fr.content_sha256_hex,
            fetched_at=fr.fetched_at,
            http_status=fr.http_status,
        )
        xml_bytes = fr.content
        conn.commit()

    if xml_bytes is None:
        record_ingestion_error(
            conn,
            stage="fetch",
            error_type="missing_bytes",
            message="no XML bytes available for parse",
            accession_number=acc,
        )
        conn.commit()
        return "fetch_failed"

    if not config.force_reparse and _filing_parsed_clean(conn, acc):
        conn.commit()
        return "skipped"

    try:
        parsed = parse_form4_xml(xml_bytes)
    except Exception as e:
        record_ingestion_error(
            conn,
            stage="parse",
            error_type=type(e).__name__,
            message=str(e),
            accession_number=acc,
        )
        conn.commit()
        return "parse_failed"

    try:
        upsert_filing_and_transactions(
            conn,
            acc,
            parsed,
            parsed_at=parsed_at_now(),
            parse_error=None,
        )
        conn.commit()
        return "ingested"
    except Exception as e:
        conn.rollback()
        record_ingestion_error(
            conn,
            stage="load",
            error_type=type(e).__name__,
            message=str(e),
            accession_number=acc,
        )
        conn.commit()
        return "parse_failed"


def ingest_master_index_row(
    conn: Any,
    client: EdgarClient,
    row: MasterIndexRow,
    config: IngestConfig,
) -> str:
    """Ingest one daily-index row (derive accession + primary path, then :func:`ingest_form4_ref`)."""
    acc = accession_from_archive_path(row.archive_path)
    if not acc:
        record_ingestion_error(
            conn,
            stage="discover",
            error_type="bad_archive_path",
            message="could not derive accession from master index path",
            accession_number=None,
            payload={"archive_path": row.archive_path},
        )
        conn.commit()
        return "discover_failed"

    primary = primary_document_from_archive_path(row.archive_path)
    if not primary:
        primary = client.resolve_primary_document_from_filing_index(row.cik, acc)
    if not primary:
        record_ingestion_error(
            conn,
            stage="discover",
            error_type="missing_primary_document",
            message="could not resolve primary document",
            accession_number=acc,
            payload={"archive_path": row.archive_path},
        )
        conn.commit()
        return "discover_failed"

    ref = Form4FilingRef(
        cik=row.cik,
        accession_number=acc,
        filing_date=row.date_filed,
        primary_document=primary,
        form=row.form_type,  # type: ignore[arg-type]
    )
    return ingest_form4_ref(conn, client, ref, config)


def ingest_date_range(
    conn: Any,
    client: EdgarClient,
    start: date,
    end: date,
    config: IngestConfig,
    *,
    cik_filter: Sequence[str] | None = None,
) -> dict[str, int]:
    """
    For each calendar day in ``[start, end]``, load the daily master index and ingest Form 4 rows.

    ``cik_filter`` (optional) restricts to those issuer CIKs (10-digit padded strings after normalization).
    """
    allowed = {str(c).strip().zfill(10) for c in cik_filter} if cik_filter else None
    counts: dict[str, int] = {
        "ingested": 0,
        "skipped": 0,
        "fetch_failed": 0,
        "parse_failed": 0,
        "discover_failed": 0,
    }
    d = start
    delta = timedelta(days=1)
    while d <= end:
        try:
            rows = client.discover_form4_for_date(d)
        except EdgarRequestError as e:
            record_ingestion_error(
                conn,
                stage="discover",
                error_type="edgar_request_error",
                message=str(e),
                accession_number=None,
                payload={"date": d.isoformat(), "url": e.url},
            )
            conn.commit()
            counts["discover_failed"] = counts.get("discover_failed", 0) + 1
            d += delta
            continue
        except Exception as e:
            record_ingestion_error(
                conn,
                stage="discover",
                error_type=type(e).__name__,
                message=str(e),
                accession_number=None,
                payload={"date": d.isoformat()},
            )
            conn.commit()
            counts["discover_failed"] = counts.get("discover_failed", 0) + 1
            d += delta
            continue

        for row in rows:
            if allowed is not None and row.cik not in allowed:
                continue
            status = ingest_master_index_row(conn, client, row, config)
            counts[status] = counts.get(status, 0) + 1

        d += delta

    return counts


def ingest_cik_submissions_for_date_range(
    conn: Any,
    client: EdgarClient,
    ciks: Sequence[str],
    start: date,
    end: date,
    config: IngestConfig,
) -> dict[str, int]:
    """Use ``data.sec.gov`` submissions for each CIK; ingest refs whose ``filing_date`` is in range."""
    counts: dict[str, int] = {
        "ingested": 0,
        "skipped": 0,
        "fetch_failed": 0,
        "parse_failed": 0,
        "discover_failed": 0,
    }
    for raw_cik in ciks:
        try:
            for ref in client.iter_form4_for_cik(raw_cik):
                if ref.filing_date < start or ref.filing_date > end:
                    continue
                status = ingest_form4_ref(conn, client, ref, config)
                counts[status] = counts.get(status, 0) + 1
        except EdgarRequestError as e:
            record_ingestion_error(
                conn,
                stage="discover",
                error_type="edgar_request_error",
                message=str(e),
                accession_number=None,
                payload={"cik": str(raw_cik), "url": e.url},
            )
            conn.commit()
        except Exception as e:
            record_ingestion_error(
                conn,
                stage="discover",
                error_type=type(e).__name__,
                message=str(e),
                accession_number=None,
                payload={"cik": str(raw_cik)},
            )
            conn.commit()
    return counts


def eastern_previous_calendar_date() -> date:
    """US/Eastern calendar date for 'yesterday' incremental runs."""
    z = ZoneInfo("America/New_York")
    now = datetime.now(z)
    return (now.date() - timedelta(days=1))
