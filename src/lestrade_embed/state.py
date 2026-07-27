from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from lestrade_embed.config import ModelSpec
from lestrade_ingest.db import connection_cursor


def get_index_state(
    conn: Any,
    *,
    accession_number: str,
    transaction_index: int,
    model_id: str,
) -> dict[str, Any] | None:
    with connection_cursor(conn) as cur:
        cur.execute(
            """
            SELECT content_hash, status
            FROM embedding_index_state
            WHERE accession_number = %s
              AND transaction_index = %s
              AND model_id = %s
            """,
            (accession_number, transaction_index, model_id),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {"content_hash": row[0], "status": row[1]}


def upsert_index_state(
    conn: Any,
    *,
    accession_number: str,
    transaction_index: int,
    spec: ModelSpec,
    content_hash: str,
    status: str,
    error_message: str | None,
) -> None:
    now = datetime.now(timezone.utc)
    with connection_cursor(conn) as cur:
        cur.execute(
            """
            INSERT INTO embedding_index_state (
                accession_number, transaction_index, model_id,
                content_hash, chroma_collection, status, error_message,
                embedded_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (accession_number, transaction_index, model_id) DO UPDATE
              SET content_hash = EXCLUDED.content_hash,
                  chroma_collection = EXCLUDED.chroma_collection,
                  status = EXCLUDED.status,
                  error_message = EXCLUDED.error_message,
                  embedded_at = EXCLUDED.embedded_at,
                  updated_at = EXCLUDED.updated_at
            """,
            (
                accession_number,
                transaction_index,
                spec.model_id,
                content_hash,
                spec.chroma_collection,
                status,
                error_message,
                now if status == "ok" else None,
                now,
            ),
        )


def needs_embed(
    conn: Any,
    *,
    accession_number: str,
    transaction_index: int,
    model_id: str,
    content_hash: str,
) -> bool:
    prev = get_index_state(
        conn,
        accession_number=accession_number,
        transaction_index=transaction_index,
        model_id=model_id,
    )
    if prev is None:
        return True
    if prev.get("status") != "ok":
        return True
    return prev.get("content_hash") != content_hash
