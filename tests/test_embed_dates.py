from datetime import date

import pytest

from lestrade_embed.dates import resolve_embed_trading_dates


def test_resolve_single_universe_date() -> None:
    assert resolve_embed_trading_dates(
        universe_trading_date="2026-05-15",
        embed_trading_dates="",
    ) == [date(2026, 5, 15)]


def test_embed_dates_override_universe() -> None:
    assert resolve_embed_trading_dates(
        universe_trading_date="2026-05-15",
        embed_trading_dates="2026-05-14, 2026-05-15",
    ) == [date(2026, 5, 14), date(2026, 5, 15)]


def test_missing_dates_raises() -> None:
    with pytest.raises(ValueError, match="LESTRADE_UNIVERSE_TRADING_DATE"):
        resolve_embed_trading_dates(universe_trading_date="", embed_trading_dates="")
