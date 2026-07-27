from __future__ import annotations

import hashlib

from lestrade_embed.config import TEMPLATE_VERSION


def content_hash(text: str) -> str:
    """SHA-256 hex of template version + canonical chunk text."""
    payload = f"{TEMPLATE_VERSION}\n{text}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def chunk_id(accession_number: str, transaction_index: int) -> str:
    return f"{accession_number}:{transaction_index}"
