from datetime import date

from lestrade_embed.universe_meta import resolve_universe_metadata_for_ticker


def test_option3_single_group() -> None:
    td = date(2026, 5, 15)
    group_map = {(td, "AAPL"): "high"}
    u_date, u_group = resolve_universe_metadata_for_ticker(group_map, [td], "AAPL")
    assert u_date == "2026-05-15"
    assert u_group == "high"


def test_option3_both_high_and_low_null_group() -> None:
    td = date(2026, 5, 15)
    group_map = {(td, "XYZ"): None}
    u_date, u_group = resolve_universe_metadata_for_ticker(group_map, [td], "xyz")
    assert u_date == "2026-05-15"
    assert u_group is None


def test_multi_day_ambiguous_groups() -> None:
    d1 = date(2026, 5, 14)
    d2 = date(2026, 5, 15)
    group_map = {(d1, "TICK"): "high", (d2, "TICK"): "low"}
    u_date, u_group = resolve_universe_metadata_for_ticker(group_map, [d1, d2], "TICK")
    assert u_date == "2026-05-14"
    assert u_group is None
