from datetime import date
from decimal import Decimal

from lestrade_embed.hash_util import content_hash
from lestrade_embed.template import render_transaction_text


def test_render_purchase_with_shares_and_price() -> None:
    text = render_transaction_text(
        {
            "insider_name": "Jane Doe",
            "insider_title": "CEO",
            "issuer_name": "Acme Corp",
            "issuer_ticker": "ACME",
            "transaction_date": date(2026, 5, 10),
            "transaction_code": "P",
            "shares": Decimal("1000"),
            "price_per_share": Decimal("12.5"),
            "total_value": Decimal("12500"),
            "direct_indirect": "D",
            "document_type": "4",
            "is_amendment": False,
        }
    )
    assert "Jane Doe" in text
    assert "CEO" in text
    assert "Acme Corp (ACME)" in text
    assert "open-market purchase" in text
    assert "1000" in text
    assert "$12.5" in text
    assert "direct ownership" in text
    assert content_hash(text) == content_hash(text)


def test_render_missing_shares_and_price() -> None:
    text = render_transaction_text(
        {
            "insider_name": "Bob",
            "issuer_name": "Co",
            "transaction_date": date(2026, 1, 1),
            "transaction_code": "A",
            "shares": None,
            "price_per_share": None,
        }
    )
    assert "grant or award" in text
    assert "not reported as scalars" in text
