from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class IngestConfig:
    """Ingestion behavior (dedup, storage, refetch)."""

    raw_storage: Literal["bytea", "s3"] = "bytea"
    """Persist raw XML in Postgres BYTEA or S3 (requires ``s3_client`` + ``s3_bucket``)."""

    force_refetch: bool = False
    """If True, download from EDGAR even when accession + SHA-256 already match."""

    force_reparse: bool = False
    """If True, parse and reload filing / transactions even when rows already exist."""

    s3_bucket: str | None = None
    s3_client: object | None = None
