from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date
from typing import Any

from lestrade_embed.chroma_store import ChromaTransactionStore
from lestrade_embed.chunks import build_chunk_records
from lestrade_embed.config import ModelSpec, get_model_spec
from lestrade_embed.models import ChunkRecord
from lestrade_embed.providers import provider_for_spec
from lestrade_embed.state import needs_embed, upsert_index_state
from lestrade_ingest.db import safe_commit
from lestrade_ingest.universe import universe_snapshot_exists_for_date

log = logging.getLogger(__name__)


def _batch_size() -> int:
    raw = (os.environ.get("LESTRADE_EMBED_BATCH_SIZE") or "32").strip()
    try:
        n = int(raw)
    except ValueError:
        n = 32
    return max(1, min(n, 256))


@dataclass
class SyncStats:
    total_chunks: int = 0
    skipped_unchanged: int = 0
    embedded: int = 0
    errors: int = 0


def _parse_chunk_id(chunk_id: str) -> tuple[str, int]:
    acc, idx_s = chunk_id.rsplit(":", 1)
    return acc, int(idx_s)


def sync_embeddings_for_model(
    conn: Any,
    *,
    model_id: str,
    trading_dates: list[date],
    persist_directory: str | None = None,
) -> SyncStats:
    """Embed in-scope transactions for ``model_id``; upsert Chroma + Postgres state."""
    spec = get_model_spec(model_id)
    stats = SyncStats()

    for td in trading_dates:
        if not universe_snapshot_exists_for_date(conn, td):
            raise ValueError(
                f"No universe_snapshot rows for {td.isoformat()}. Run universe_snapshot first."
            )

    chunks = build_chunk_records(conn, trading_dates)
    stats.total_chunks = len(chunks)
    if not chunks:
        log.info("No transactions in scope for dates %s", [d.isoformat() for d in trading_dates])
        return stats

    pending: list[ChunkRecord] = []
    for ch in chunks:
        acc, tidx = _parse_chunk_id(ch.chunk_id)
        if not needs_embed(
            conn,
            accession_number=acc,
            transaction_index=tidx,
            model_id=spec.model_id,
            content_hash=ch.content_hash,
        ):
            stats.skipped_unchanged += 1
            continue
        pending.append(ch)

    if not pending:
        log.info(
            "All %s chunks already indexed for %s",
            stats.total_chunks,
            spec.model_id,
        )
        return stats

    store = ChromaTransactionStore(spec, persist_directory=persist_directory)
    provider = provider_for_spec(spec)
    batch_size = _batch_size()

    for i in range(0, len(pending), batch_size):
        batch = pending[i : i + batch_size]
        texts = [c.text for c in batch]
        try:
            vectors = provider.embed_documents(texts)
        except Exception as e:
            log.exception("Embedding batch failed for %s: %s", spec.model_id, e)
            for ch in batch:
                acc, tidx = _parse_chunk_id(ch.chunk_id)
                upsert_index_state(
                    conn,
                    accession_number=acc,
                    transaction_index=tidx,
                    spec=spec,
                    content_hash=ch.content_hash,
                    status="error",
                    error_message=str(e)[:2000],
                )
                stats.errors += 1
            safe_commit(conn)
            continue

        if len(vectors) != len(batch):
            raise RuntimeError(
                f"Provider returned {len(vectors)} vectors for {len(batch)} texts"
            )

        try:
            store.upsert(
                ids=[c.chunk_id for c in batch],
                embeddings=vectors,
                documents=texts,
                metadatas=[c.metadata for c in batch],
            )
        except Exception as e:
            log.exception("Chroma upsert failed: %s", e)
            for ch in batch:
                acc, tidx = _parse_chunk_id(ch.chunk_id)
                upsert_index_state(
                    conn,
                    accession_number=acc,
                    transaction_index=tidx,
                    spec=spec,
                    content_hash=ch.content_hash,
                    status="error",
                    error_message=str(e)[:2000],
                )
                stats.errors += 1
            safe_commit(conn)
            continue

        for ch in batch:
            acc, tidx = _parse_chunk_id(ch.chunk_id)
            upsert_index_state(
                conn,
                accession_number=acc,
                transaction_index=tidx,
                spec=spec,
                content_hash=ch.content_hash,
                status="ok",
                error_message=None,
            )
            stats.embedded += 1
        safe_commit(conn)
        log.info(
            "Embedded batch %s-%s / %s for %s",
            i + 1,
            i + len(batch),
            len(pending),
            spec.model_id,
        )

    log.info(
        "Sync done model=%s collection=%s total=%s embedded=%s skipped=%s errors=%s",
        spec.model_id,
        spec.chroma_collection,
        stats.total_chunks,
        stats.embedded,
        stats.skipped_unchanged,
        stats.errors,
    )
    return stats
