from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Generator


@contextmanager
def connection_cursor(conn: Any) -> Generator[Any, None, None]:
    """Use a dedicated cursor; commit/rollback is the caller's responsibility."""
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur.close()


def ensure_connection(conn: Any) -> Any:
    if conn is None:
        raise ValueError("database connection is required")
    return conn
