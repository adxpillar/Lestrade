from __future__ import annotations

import json
import random
import time
from datetime import date
from typing import Any, Iterator

import requests

from lestrade_edgar.cik import pad_cik
from lestrade_edgar.discovery import (
    daily_master_index_url,
    iter_form4_from_submissions,
    parse_filing_index_for_primary_xml,
    parse_master_idx_form4,
    read_filing_index_text,
    submissions_json_url,
)
from lestrade_edgar.exceptions import EdgarRequestError
from lestrade_edgar.fetch import build_fetch_result, filing_index_url, primary_document_url
from lestrade_edgar.models import FetchResult, Form4FilingRef, MasterIndexRow, StoredRawXml
from lestrade_edgar.rate_limit import MinIntervalLimiter
from lestrade_edgar.storage import store_raw_xml, suggested_s3_key


class EdgarClient:
    """
    SEC EDGAR HTTP access with required User-Agent, rate limiting, and retries.

    Use ``data.sec.gov`` for submissions JSON; ``www.sec.gov`` for Archives
    (daily index, filing documents) per SEC guidance.
    """

    def __init__(
        self,
        *,
        app_name: str,
        contact_email: str,
        min_interval_s: float = 0.12,
        max_attempts: int = 6,
        base_backoff_s: float = 1.0,
        max_backoff_s: float = 60.0,
        timeout_s: float = 60.0,
        session: requests.Session | None = None,
    ) -> None:
        an = (app_name or "").strip()
        em = (contact_email or "").strip()
        if not an or not em:
            raise ValueError("app_name and contact_email are required (SEC User-Agent policy)")
        if min_interval_s < 0:
            raise ValueError("min_interval_s must be >= 0")
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")

        self._min_interval_s = min_interval_s
        self._max_attempts = max_attempts
        self._base_backoff_s = base_backoff_s
        self._max_backoff_s = max_backoff_s
        self._timeout = timeout_s
        self._limiter = MinIntervalLimiter(min_interval_s)
        self._session = session or requests.Session()
        self._session.headers.update(
            {
                "User-Agent": f"{an} {em}",
                "Accept-Encoding": "gzip, deflate",
                "Accept": "*/*",
            }
        )

    def get_bytes(self, url: str) -> tuple[bytes, int]:
        """GET URL; return (body, status). Retries on 429 / 5xx and transient errors."""
        last_status: int | None = None
        for attempt in range(self._max_attempts):
            self._limiter.wait_turn()
            try:
                resp = self._session.get(url, timeout=self._timeout)
            except requests.RequestException as e:
                if attempt >= self._max_attempts - 1:
                    raise EdgarRequestError(f"EDGAR GET failed: {e}", url=url) from e
                self._sleep_backoff(attempt)
                continue

            last_status = int(resp.status_code)
            body = resp.content

            if last_status == 429 or (500 <= last_status < 600):
                if attempt >= self._max_attempts - 1:
                    return body, last_status
                ra = resp.headers.get("Retry-After")
                if ra is not None:
                    self._sleep_retry_after(ra, attempt)
                else:
                    self._sleep_backoff(attempt)
                continue

            return body, last_status

        raise EdgarRequestError("EDGAR GET exhausted retries", url=url)

    def get_json(self, url: str) -> Any:
        body, status = self.get_bytes(url)
        if status != 200:
            raise EdgarRequestError(
                f"expected 200, got {status}",
                url=url,
                status_code=status,
            )
        return json.loads(body.decode("utf-8"))

    def get_text(self, url: str) -> str:
        body, status = self.get_bytes(url)
        if status != 200:
            raise EdgarRequestError(
                f"expected 200, got {status}",
                url=url,
                status_code=status,
            )
        return body.decode("utf-8", errors="replace")

    def fetch_submissions(self, cik: str | int) -> dict[str, Any]:
        """Submissions JSON for one issuer (``data.sec.gov``)."""
        padded = pad_cik(cik)
        return self.get_json(submissions_json_url(padded))

    def iter_form4_for_cik(self, cik: str | int) -> Iterator[Form4FilingRef]:
        """Discover Form 4 / 4/A from submissions ``filings.recent``."""
        payload = self.fetch_submissions(cik)
        yield from iter_form4_from_submissions(payload, cik=cik)

    def fetch_daily_master_index(self, d: date) -> str:
        """Download daily ``masterYYYYMMDD.idx`` body as text (``www.sec.gov``)."""
        url = daily_master_index_url(d)
        return self.get_text(url)

    def discover_form4_for_date(self, d: date) -> list[MasterIndexRow]:
        """Parse daily master index for Form types 4 and 4/A only."""
        text = self.fetch_daily_master_index(d)
        return parse_master_idx_form4(text)

    def resolve_primary_document_from_filing_index(
        self,
        cik: str | int,
        accession_number: str,
    ) -> str | None:
        """
        Load ``{accession}-index.htm`` and pick a best-effort primary Form 4 XML href.

        Use when you have CIK + accession from ``master.idx`` but not ``primaryDocument``.
        """
        url = filing_index_url(cik, accession_number)
        body, status = self.get_bytes(url)
        if status != 200:
            return None
        return parse_filing_index_for_primary_xml(read_filing_index_text(body))

    def fetch_primary_document(
        self,
        cik: str | int,
        accession_number: str,
        primary_document: str,
    ) -> FetchResult:
        """
        Download primary document bytes; compute SHA-256; include HTTP status.

        Non-retryable HTTP codes (e.g. 404) are returned as-is without raising.
        """
        url = primary_document_url(cik, accession_number, primary_document)
        body, status = self.get_bytes(url)
        return build_fetch_result(body, http_status=status, url=url)

    def fetch_primary_document_and_store(
        self,
        cik: str | int,
        accession_number: str,
        primary_document: str,
        *,
        mode: str,
        s3_client: object | None = None,
        bucket: str | None = None,
        s3_key: str | None = None,
    ) -> StoredRawXml:
        """
        Fetch, hash, then store to S3 or inline BYTEA per DATA_CONTRACT.

        ``mode`` is ``"s3"`` or ``"bytea"``. For S3, pass ``boto3.client("s3")``;
        if ``s3_key`` is omitted, :func:`suggested_s3_key` is used.
        """
        padded = pad_cik(cik)
        fr = self.fetch_primary_document(padded, accession_number, primary_document)
        if fr.http_status != 200:
            raise EdgarRequestError(
                f"expected HTTP 200 when storing raw XML, got {fr.http_status}",
                url=fr.url,
                status_code=fr.http_status,
            )
        if mode == "bytea":
            return store_raw_xml(fr, mode="bytea")
        if mode != "s3":
            raise ValueError('mode must be "s3" or "bytea"')
        key = s3_key or suggested_s3_key(
            cik_padded=padded,
            accession_number=accession_number,
            primary_document=primary_document,
            content_sha256_hex=fr.content_sha256_hex,
        )
        return store_raw_xml(
            fr,
            mode="s3",
            s3_client=s3_client,
            bucket=bucket,
            key=key,
        )

    def _sleep_backoff(self, attempt: int) -> None:
        exp = min(self._max_backoff_s, self._base_backoff_s * (2**attempt))
        jitter = random.uniform(0, exp * 0.25)
        time.sleep(min(self._max_backoff_s, exp + jitter))

    def _sleep_retry_after(self, header_value: str, attempt: int) -> None:
        try:
            sec = float(header_value)
        except ValueError:
            self._sleep_backoff(attempt)
            return
        sec = max(0.0, min(sec, self._max_backoff_s))
        time.sleep(sec)
