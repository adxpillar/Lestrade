from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any, Iterable

from lestrade_edgar.cik import pad_cik
from lestrade_edgar.exceptions import EdgarRequestError
from lestrade_edgar.submissions_company import issuer_metadata_from_submissions
from lestrade_ingest.db import connection_cursor

_log = logging.getLogger(__name__)


def distinct_ciks_from_universe_snapshot(conn: Any, trading_date: date) -> list[str]:
    with connection_cursor(conn) as cur:
        cur.execute(
            """
            SELECT DISTINCT cik::text
            FROM universe_snapshot
            WHERE trading_date = %s AND cik IS NOT NULL
            ORDER BY 1
            """,
            (trading_date,),
        )
        rows = cur.fetchall() or []
    out: list[str] = []
    for (raw,) in rows:
        s = (raw or "").strip()
        if s:
            out.append(pad_cik(s))
    return out


def distinct_ciks_from_security_master(conn: Any, *, limit: int) -> list[str]:
    lim = max(1, min(limit, 50_000))
    with connection_cursor(conn) as cur:
        cur.execute(
            """
            SELECT cik::text
            FROM security_master
            WHERE cik IS NOT NULL
            ORDER BY ticker
            LIMIT %s
            """,
            (lim,),
        )
        rows = cur.fetchall() or []
    out: list[str] = []
    for (raw,) in rows:
        s = (raw or "").strip()
        if s:
            out.append(pad_cik(s))
    return out


def upsert_issuer_sec_profile(
    conn: Any,
    *,
    cik: str,
    fields: dict[str, Any],
    enrichment_status: str,
    error_message: str | None,
    source: str = "sec_submissions",
) -> None:
    now = datetime.now(timezone.utc)
    cik_p = pad_cik(cik)
    with connection_cursor(conn) as cur:
        cur.execute(
            """
            INSERT INTO issuer_sec_profile (
                cik, entity_name, entity_type, sic_code, sic_description,
                state_of_incorporation, fiscal_year_end, tickers, exchanges,
                fetched_at, updated_at, source, enrichment_status, error_message
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (cik) DO UPDATE SET
                entity_name = EXCLUDED.entity_name,
                entity_type = EXCLUDED.entity_type,
                sic_code = EXCLUDED.sic_code,
                sic_description = EXCLUDED.sic_description,
                state_of_incorporation = EXCLUDED.state_of_incorporation,
                fiscal_year_end = EXCLUDED.fiscal_year_end,
                tickers = EXCLUDED.tickers,
                exchanges = EXCLUDED.exchanges,
                fetched_at = EXCLUDED.fetched_at,
                updated_at = EXCLUDED.updated_at,
                source = EXCLUDED.source,
                enrichment_status = EXCLUDED.enrichment_status,
                error_message = EXCLUDED.error_message
            """,
            (
                cik_p,
                fields.get("entity_name"),
                fields.get("entity_type"),
                fields.get("sic_code"),
                fields.get("sic_description"),
                fields.get("state_of_incorporation"),
                fields.get("fiscal_year_end"),
                fields.get("tickers"),
                fields.get("exchanges"),
                now,
                now,
                source,
                enrichment_status,
                error_message,
            ),
        )


def enrich_issuer_sec_profiles(
    conn: Any,
    client: Any,
    ciks: Iterable[str],
    *,
    commit_each: bool = False,
) -> tuple[int, int]:
    """
    Fetch submissions JSON per CIK and upsert ``issuer_sec_profile``.

    Returns: (ok_count, error_count)
    """
    ok = err = 0
    for cik in ciks:
        cik_p = pad_cik(cik)
        empty_fields: dict[str, Any] = {
            "entity_name": None,
            "entity_type": None,
            "sic_code": None,
            "sic_description": None,
            "state_of_incorporation": None,
            "fiscal_year_end": None,
            "tickers": None,
            "exchanges": None,
        }
        try:
            payload = client.fetch_submissions(cik_p)
            if not isinstance(payload, dict) or payload.get("cik") is None:
                raise ValueError("submissions JSON missing expected company fields")
            fields = issuer_metadata_from_submissions(payload)
            upsert_issuer_sec_profile(
                conn,
                cik=cik_p,
                fields=fields,
                enrichment_status="ok",
                error_message=None,
            )
            ok += 1
        except (EdgarRequestError, ValueError, OSError, TypeError) as e:
            err += 1
            msg = str(e)[:2000]
            _log.warning("SEC issuer profile fetch failed for CIK %s: %s", cik_p, e)
            upsert_issuer_sec_profile(
                conn,
                cik=cik_p,
                fields=empty_fields,
                enrichment_status="error",
                error_message=msg,
            )
        if commit_each:
            conn.commit()
    return ok, err
