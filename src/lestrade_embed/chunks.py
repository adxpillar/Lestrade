from __future__ import annotations

from datetime import date
from typing import Any, Iterator

from lestrade_embed.hash_util import chunk_id, content_hash
from lestrade_embed.models import ChunkRecord
from lestrade_embed.template import render_transaction_text
from lestrade_embed.universe_groups import load_universe_group_by_ticker
from lestrade_embed.universe_meta import resolve_universe_metadata_for_ticker
from lestrade_ingest.db import connection_cursor


def _row_to_dict(cols: list[str], row: tuple) -> dict[str, Any]:
    return dict(zip(cols, row))


def iter_transaction_rows(
    conn: Any,
    trading_dates: list[date],
) -> Iterator[dict[str, Any]]:
    """Yield filing + transaction dicts in scope for universe session date(s)."""
    if not trading_dates:
        return
    sql = """
        SELECT
            f.accession_number,
            t.transaction_index,
            f.document_type,
            f.is_amendment,
            f.issuer_cik,
            f.issuer_name,
            f.issuer_ticker,
            f.insider_cik,
            f.insider_name,
            f.insider_title,
            fr.filing_date,
            t.transaction_category,
            t.security_title,
            t.transaction_date,
            t.transaction_code,
            t.acquired_disposed_code,
            t.shares,
            t.price_per_share,
            t.total_value,
            t.direct_indirect
        FROM form4_transaction t
        JOIN filing f ON f.accession_number = t.accession_number
        JOIN filing_raw fr ON fr.accession_number = f.accession_number
        WHERE EXISTS (
            SELECT 1
            FROM universe_snapshot u
            WHERE u.trading_date = ANY(%s)
              AND (
                    (u.cik IS NOT NULL AND u.cik = f.issuer_cik)
                 OR (
                        u.ticker IS NOT NULL
                    AND upper(trim(u.ticker)) = upper(trim(coalesce(f.issuer_ticker, '')))
                 )
              )
        )
        ORDER BY f.accession_number, t.transaction_index
    """
    with connection_cursor(conn) as cur:
        cur.execute(sql, (trading_dates,))
        cols = [d[0] for d in cur.description]
        for row in cur.fetchall() or []:
            yield _row_to_dict(cols, row)


def build_chunk_records(
    conn: Any,
    trading_dates: list[date],
) -> list[ChunkRecord]:
    group_map = load_universe_group_by_ticker(conn, trading_dates)
    out: list[ChunkRecord] = []
    for row in iter_transaction_rows(conn, trading_dates):
        text = render_transaction_text(row)
        cid = chunk_id(row["accession_number"], int(row["transaction_index"]))
        chash = content_hash(text)
        u_date, u_group = resolve_universe_metadata_for_ticker(
            group_map, trading_dates, row.get("issuer_ticker")
        )
        filing_date = row.get("filing_date")
        tx_date = row.get("transaction_date")
        meta: dict[str, Any] = {
            "accession_number": row["accession_number"],
            "transaction_index": int(row["transaction_index"]),
            "issuer_cik": row.get("issuer_cik"),
            "issuer_ticker": row.get("issuer_ticker"),
            "insider_cik": row.get("insider_cik"),
            "insider_name": row.get("insider_name"),
            "document_type": row.get("document_type"),
            "is_amendment": bool(row.get("is_amendment")),
            "transaction_category": row.get("transaction_category"),
            "transaction_code": row.get("transaction_code"),
            "acquired_disposed_code": row.get("acquired_disposed_code"),
            "universe_trading_date": u_date,
            "universe_group": u_group,
        }
        if isinstance(filing_date, date):
            meta["filing_date"] = filing_date.isoformat()
        if isinstance(tx_date, date):
            meta["transaction_date"] = tx_date.isoformat()
        out.append(ChunkRecord(chunk_id=cid, text=text, content_hash=chash, metadata=meta))
    return out
