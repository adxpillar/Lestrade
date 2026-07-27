"""Guards for Postgres NUMERIC(18,6) / BIGINT bounds on Stooq-derived bars."""

from lestrade_enrich.stooq import sanitize_ohlc_for_numeric_18_6, sanitize_volume_for_bigint


def test_sanitize_ohlc_accepts_typical_price() -> None:
    assert sanitize_ohlc_for_numeric_18_6(123.456789) == 123.456789
    assert sanitize_ohlc_for_numeric_18_6(0.0) == 0.0


def test_sanitize_ohlc_rejects_out_of_range() -> None:
    assert sanitize_ohlc_for_numeric_18_6(1e12) is None
    assert sanitize_ohlc_for_numeric_18_6(-1e12) is None
    assert sanitize_ohlc_for_numeric_18_6(float("nan")) is None
    assert sanitize_ohlc_for_numeric_18_6(float("inf")) is None


def test_sanitize_volume_bigint_bounds() -> None:
    assert sanitize_volume_for_bigint(1_000_000) == 1_000_000
    assert sanitize_volume_for_bigint(9223372036854775807) == 9223372036854775807
    assert sanitize_volume_for_bigint(9223372036854775808) is None
    assert sanitize_volume_for_bigint(-9223372036854775808) == -9223372036854775808
    assert sanitize_volume_for_bigint(-9223372036854775809) is None
