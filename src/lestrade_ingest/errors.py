from __future__ import annotations

import logging
from typing import Any

import psycopg2
from psycopg2.extras import Json

from lestrade_ingest.db import connection_cursor

_log = logging.getLogger(__name__)


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
    try:
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
    except (psycopg2.InterfaceError, psycopg2.OperationalError) as ex:
        _log.warning(
            "Could not write ingestion_errors row (connection issue): %s | stage=%s error_type=%s accession=%s msg=%s",
            ex,
            stage,
            error_type,
            accession_number,
            (message or "")[:500],
        )
