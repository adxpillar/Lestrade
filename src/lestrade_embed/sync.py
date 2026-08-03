from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date
from typing import Any

from lestrade_embed.chroma_store import ChromaTransactionStore
from lestrade_embed.chunks import build_chunk_records
from lestrade_embed.config import get_model_spec
from lestrade_embed.models import ChunkRecord
from lestrade_embed.providers import provider_for_spec
from lestrade_embed.state import (
    load_index_states_for_model,
    needs_embed_from_prev,
    upsert_index_state,
)
from lestrade_ingest.db import is_connection_exception, safe_commit, safe_rollback
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


def _ping(conn: Any) -> None:
    from lestrade_ingest.db import connection_cursor

    with connection_cursor(conn) as cur:
        cur.execute("SELECT 1")
        cur.fetchone()


def sync_embeddings_for_model(
    conn: Any,
    *,
    model_id: str,
    trading_dates: list[date],
    persist_directory: str | None = None,
    reconnect: Any | None = None,
) -> SyncStats:
    """Embed in-scope transactions for ``model_id``; upsert Chroma + Postgres state.

    ``reconnect`` is an optional zero-arg callable that returns a fresh DB connection
    (used after Supabase pooler drops idle/long-lived sessions).
    """
    spec = get_model_spec(model_id)
    stats = SyncStats()

    def _refresh_conn() -> Any:
        nonlocal conn
        if reconnect is None:
            raise RuntimeError(
                "Database connection dropped (often Supabase pooler on long jobs). "
                "Use the direct Postgres URI (port 5432) in AIRFLOW_CONN_LESTRADE_RDS, "
                "or pass reconnect= to sync_embeddings_for_model."
            ) from None
        try:
            safe_rollback(conn)
            conn.close()
        except Exception:
            pass
        conn = reconnect()
        log.warning("Refreshed Postgres connection after drop")
        return conn

    def _with_conn(fn: Any) -> Any:
        try:
            return fn(conn)
        except Exception as e:
            if not is_connection_exception(e):
                raise
            _refresh_conn()
            return fn(conn)

    for td in trading_dates:
        if not _with_conn(lambda c: universe_snapshot_exists_for_date(c, td)):
            raise ValueError(
                f"No universe_snapshot rows for {td.isoformat()}. Run universe_snapshot first."
            )

    chunks = _with_conn(lambda c: build_chunk_records(c, trading_dates))
    stats.total_chunks = len(chunks)
    if not chunks:
        log.info("No transactions in scope for dates %s", [d.isoformat() for d in trading_dates])
        return stats

    keys = [_parse_chunk_id(ch.chunk_id) for ch in chunks]
    states = _with_conn(
        lambda c: load_index_states_for_model(c, model_id=spec.model_id, keys=keys)
    )

    pending: list[ChunkRecord] = []
    for ch in chunks:
        acc, tidx = _parse_chunk_id(ch.chunk_id)
        if not needs_embed_from_prev(states.get((acc, tidx)), content_hash=ch.content_hash):
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

    # Load embedder / Chroma before more DB work so a long model download
    # does not hold an idle pooler session.
    store = ChromaTransactionStore(spec, persist_directory=persist_directory)
    provider = provider_for_spec(spec)
    batch_size = _batch_size()

    try:
        _ping(conn)
    except Exception as e:
        if is_connection_exception(e):
            _refresh_conn()
        else:
            raise

    for i in range(0, len(pending), batch_size):
        batch = pending[i : i + batch_size]
        texts = [c.text for c in batch]
        try:
            vectors = provider.embed_documents(texts)
        except Exception as e:
            log.exception("Embedding batch failed for %s: %s", spec.model_id, e)

            def _mark_errors(c: Any) -> None:
                for ch in batch:
                    acc, tidx = _parse_chunk_id(ch.chunk_id)
                    upsert_index_state(
                        c,
                        accession_number=acc,
                        transaction_index=tidx,
                        spec=spec,
                        content_hash=ch.content_hash,
                        status="error",
                        error_message=str(e)[:2000],
                    )
                    stats.errors += 1
                safe_commit(c)

            _with_conn(_mark_errors)
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

            def _mark_chroma_errors(c: Any) -> None:
                for ch in batch:
                    acc, tidx = _parse_chunk_id(ch.chunk_id)
                    upsert_index_state(
                        c,
                        accession_number=acc,
                        transaction_index=tidx,
                        spec=spec,
                        content_hash=ch.content_hash,
                        status="error",
                        error_message=str(e)[:2000],
                    )
                    stats.errors += 1
                safe_commit(c)

            _with_conn(_mark_chroma_errors)
            continue

        def _mark_ok(c: Any) -> None:
            for ch in batch:
                acc, tidx = _parse_chunk_id(ch.chunk_id)
                upsert_index_state(
                    c,
                    accession_number=acc,
                    transaction_index=tidx,
                    spec=spec,
                    content_hash=ch.content_hash,
                    status="ok",
                    error_message=None,
                )
                stats.embedded += 1
            safe_commit(c)

        _with_conn(_mark_ok)
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
