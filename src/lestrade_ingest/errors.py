from __future__ import annotations

from typing import Any

from psycopg2.extras import Json

from lestrade_ingest.db import connection_cursor


def record_ingestion_error(
    conn: Any,
    *,
    stage: str,
    error_type: str,
    message: str,
    accession_number: str | None = None,
    payload: dict[str, Any] | None = None,
) -> None:
    """Insert one row into ``ingestion_errors`` (DATA_CONTRACT §8)."""
    with connection_cursor(conn) as cur:
        cur.execute(
            """
            INSERT INTO ingestion_errors (accession_number, stage, error_type, message, payload)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                accession_number,
                stage,
                error_type,
                message,
                Json(payload) if payload is not None else None,
            ),
        )
