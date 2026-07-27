from __future__ import annotations

from datetime import date

def _normalize_ticker(t: str) -> str:
    return (t or "").strip().upper()


def resolve_universe_metadata_for_ticker(
    group_map: dict[tuple[date, str], str | None],
    trading_dates: list[date],
    ticker: str | None,
) -> tuple[str | None, str | None]:
    """
    Pick universe_trading_date (earliest matching session) and universe_group.

    universe_group is set only when exactly one distinct non-null group exists
    across all matching (date, ticker) keys; otherwise None (Option 3 / multi-day).
    """
    if not ticker or not trading_dates:
        return None, None
    nt = _normalize_ticker(ticker)
    groups: set[str] = set()
    matched_dates: list[date] = []
    for td in sorted(trading_dates):
        if (td, nt) in group_map:
            matched_dates.append(td)
            g = group_map.get((td, nt))
            if g is not None:
                groups.add(g)
    if not matched_dates:
        return None, None
    primary_date = matched_dates[0].isoformat()
    if len(groups) == 1:
        return primary_date, next(iter(groups))
    return primary_date, None
