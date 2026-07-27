from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ChunkRecord:
    """One embeddable transaction line."""

    chunk_id: str
    text: str
    content_hash: str
    metadata: dict[str, Any]
