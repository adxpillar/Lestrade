from __future__ import annotations

from datetime import datetime
from typing import Any

from lestrade_ingest.db import connection_cursor
from lestrade_ingest.form4_parser import ParsedFiling


def filing_row_exists(conn: Any, accession_number: str) -> bool:
    with connection_cursor(conn) as cur:
        cur.execute(
            "SELECT 1 FROM filing WHERE accession_number = %s LIMIT 1",
            (accession_number,),
        )
        return cur.fetchone() is not None


def upsert_filing_and_transactions(
    conn: Any,
    accession_number: str,
    parsed: ParsedFiling,
    *,
    parsed_at: datetime,
    parse_error: str | None = None,
) -> None:
    """Replace ``form4_transaction`` rows and upsert ``filing`` for this accession."""
    if parse_error and parsed.parse_warnings:
        full_parse_error = parse_error + " | " + "; ".join(parsed.parse_warnings)
    elif parse_error:
        full_parse_error = parse_error
    elif parsed.parse_warnings:
        full_parse_error = "; ".join(parsed.parse_warnings)
    else:
        full_parse_error = None

    with connection_cursor(conn) as cur:
        cur.execute(
            """
            INSERT INTO filing (
                accession_number, document_type, is_amendment, schema_version, period_of_report,
                issuer_cik, issuer_name, issuer_ticker, insider_cik, insider_name, insider_title,
                parsed_at, parse_error
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (accession_number) DO UPDATE SET
                document_type = EXCLUDED.document_type,
                is_amendment = EXCLUDED.is_amendment,
                schema_version = EXCLUDED.schema_version,
                period_of_report = EXCLUDED.period_of_report,
                issuer_cik = EXCLUDED.issuer_cik,
                issuer_name = EXCLUDED.issuer_name,
                issuer_ticker = EXCLUDED.issuer_ticker,
                insider_cik = EXCLUDED.insider_cik,
                insider_name = EXCLUDED.insider_name,
                insider_title = EXCLUDED.insider_title,
                parsed_at = EXCLUDED.parsed_at,
                parse_error = EXCLUDED.parse_error
            """,
            (
                accession_number,
                parsed.document_type,
                parsed.is_amendment,
                parsed.schema_version,
                parsed.period_of_report,
                parsed.issuer_cik,
                parsed.issuer_name,
                parsed.issuer_ticker,
                parsed.insider_cik,
                parsed.insider_name,
                parsed.insider_title,
                parsed_at,
                full_parse_error,
            ),
        )

        cur.execute(
            "DELETE FROM form4_transaction WHERE accession_number = %s",
            (accession_number,),
        )

        for tx in parsed.transactions:
            cur.execute(
                """
                INSERT INTO form4_transaction (
                    accession_number, transaction_index, transaction_category, security_title,
                    transaction_date, transaction_code, acquired_disposed_code, shares,
                    price_per_share, total_value, direct_indirect, footnote_ids
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    accession_number,
                    tx.transaction_index,
                    tx.transaction_category,
                    tx.security_title,
                    tx.transaction_date,
                    tx.transaction_code,
                    tx.acquired_disposed_code,
                    tx.shares,
                    tx.price_per_share,
                    tx.total_value,
                    tx.direct_indirect,
                    tx.footnote_ids if tx.footnote_ids else None,
                ),
            )
