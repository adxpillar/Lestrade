from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any


def _fmt_decimal(x: Decimal | None) -> str | None:
    if x is None:
        return None
    s = format(x, "f")
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s or "0"


def _action_phrase(code: str | None, ad_code: str | None) -> str:
    c = (code or "").strip().upper()
    ad = (ad_code or "").strip().upper()
    if c == "P":
        return "open-market purchase"
    if c == "S":
        return "open-market sale"
    if c == "A":
        return "grant or award"
    if c == "M":
        return "option exercise"
    if c == "G":
        return "gift"
    if ad == "A":
        return "acquisition"
    if ad == "D":
        return "disposition"
    if c:
        return f"transaction code {c}"
    return "insider transaction"


def _ownership_phrase(direct_indirect: str | None) -> str | None:
    if not direct_indirect:
        return None
    d = direct_indirect.strip().lower()
    if d in ("d", "direct"):
        return "direct ownership"
    if d in ("i", "indirect"):
        return "indirect ownership"
    return f"ownership: {direct_indirect}"


def render_transaction_text(row: dict[str, Any]) -> str:
    """
    Deterministic English sentence for one form4_transaction row (+ filing fields).
    Never invents numeric values when shares or price are missing.
    """
    person = (row.get("insider_name") or "an insider").strip()
    title = (row.get("insider_title") or "").strip()
    company = (row.get("issuer_name") or "the issuer").strip()
    ticker = (row.get("issuer_ticker") or "").strip()
    if ticker:
        company = f"{company} ({ticker})"

    tx_date = row.get("transaction_date")
    if isinstance(tx_date, date):
        when = tx_date.isoformat()
    else:
        when = str(tx_date) if tx_date else "an undisclosed date"

    category = (row.get("transaction_category") or "").strip()
    cat_note = " (derivative)" if category == "derivative" else ""

    action = _action_phrase(row.get("transaction_code"), row.get("acquired_disposed_code"))

    shares = row.get("shares")
    price = row.get("price_per_share")
    total = row.get("total_value")

    parts = [f"On {when}, {person}"]
    if title:
        parts.append(f", {title},")
    parts.append(f" at {company} reported a {action}{cat_note}.")

    if shares is not None and price is not None:
        parts.append(
            f" Amount: {_fmt_decimal(shares)} shares at ${_fmt_decimal(price)} per share."
        )
        if total is not None:
            parts.append(f" Total value about ${_fmt_decimal(total)}.")
    elif shares is not None:
        parts.append(f" Share amount: {_fmt_decimal(shares)}; price per share not reported.")
    elif price is not None:
        parts.append(f" Price per share: ${_fmt_decimal(price)}; share amount not reported.")
    else:
        parts.append(" Share amount and price were not reported as scalars.")

    own = _ownership_phrase(row.get("direct_indirect"))
    if own:
        parts.append(f" {own}.")

    doc = (row.get("document_type") or "").strip()
    if row.get("is_amendment"):
        parts.append(" Filing is a Form 4/A amendment.")
    elif doc:
        parts.append(f" Document type: {doc}.")

    return "".join(parts).replace("  ", " ").strip()
