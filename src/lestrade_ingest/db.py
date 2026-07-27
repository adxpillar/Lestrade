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


def safe_commit(conn: Any) -> None:
    """Commit if the connection is still usable; ignore failures (e.g. connection already closed)."""
    try:
        if conn is None or getattr(conn, "closed", 1) != 0:
            return
        conn.commit()
    except Exception:
        pass


def safe_rollback(conn: Any) -> None:
    """Rollback if the connection is still usable; ignore failures."""
    try:
        if conn is None or getattr(conn, "closed", 1) != 0:
            return
        conn.rollback()
    except Exception:
        pass


def is_connection_exception(exc: BaseException) -> bool:
    """True if ``exc`` indicates a broken or lost DB connection (caller may need a fresh connection)."""
    try:
        import psycopg2
    except ImportError:
        return False
    return isinstance(exc, (psycopg2.InterfaceError, psycopg2.OperationalError))
