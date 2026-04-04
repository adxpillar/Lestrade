from __future__ import annotations

from typing import Literal

from lestrade_edgar.models import FetchResult, StoredRawXml


def store_raw_xml(
    fetch: FetchResult,
    *,
    mode: Literal["s3", "bytea"],
    s3_client: object | None = None,
    bucket: str | None = None,
    key: str | None = None,
) -> StoredRawXml:
    """
    Persist raw bytes per DATA_CONTRACT: either S3 URI or BYTEA, not both.

    For ``mode="s3"``, ``s3_client``, ``bucket``, and ``key`` are required.
    """
    if mode == "bytea":
        return StoredRawXml(
            content_sha256_hex=fetch.content_sha256_hex,
            http_status=fetch.http_status,
            fetched_at=fetch.fetched_at,
            raw_xml_s3_uri=None,
            raw_xml=fetch.content,
        )

    if s3_client is None or not bucket or not key:
        raise ValueError('mode="s3" requires s3_client, bucket, and key')
    put = getattr(s3_client, "put_object", None)
    if put is None:
        raise TypeError("s3_client must support put_object")
    put(
        Bucket=bucket,
        Key=key,
        Body=fetch.content,
        ContentType="application/xml",
    )
    uri = f"s3://{bucket}/{key}"
    return StoredRawXml(
        content_sha256_hex=fetch.content_sha256_hex,
        http_status=fetch.http_status,
        fetched_at=fetch.fetched_at,
        raw_xml_s3_uri=uri,
        raw_xml=None,
    )


def suggested_s3_key(
    *,
    cik_padded: str,
    accession_number: str,
    primary_document: str,
    content_sha256_hex: str,
) -> str:
    """Stable object key: version by hash for idempotency."""
    safe_doc = primary_document.replace("\\", "/").split("/")[-1]
    nodash = accession_number.replace("-", "")
    return f"raw_xml/{cik_padded}/{nodash}/{content_sha256_hex[:16]}_{safe_doc}"
