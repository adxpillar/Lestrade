from __future__ import annotations

import os
from datetime import date


def _parse_date_list(raw: str) -> list[date]:
    out: list[date] = []
    for part in raw.split(","):
        p = part.strip()
        if not p:
            continue
        out.append(date.fromisoformat(p[:10]))
    return sorted(set(out))


def resolve_embed_trading_dates(
    *,
    universe_trading_date: str | None = None,
    embed_trading_dates: str | None = None,
) -> list[date]:
    """
    Resolve session date(s) for embed scope.

    ``LESTRADE_EMBED_TRADING_DATES`` (comma-separated) wins over
    ``LESTRADE_UNIVERSE_TRADING_DATE`` when both are set.
    """
    multi = (embed_trading_dates if embed_trading_dates is not None else os.environ.get("LESTRADE_EMBED_TRADING_DATES") or "").strip()
    single = (universe_trading_date if universe_trading_date is not None else os.environ.get("LESTRADE_UNIVERSE_TRADING_DATE") or "").strip()
    if multi:
        dates = _parse_date_list(multi)
        if not dates:
            raise ValueError("LESTRADE_EMBED_TRADING_DATES is set but contains no valid dates")
        return dates
    if single:
        return [date.fromisoformat(single[:10])]
    raise ValueError(
        "Set LESTRADE_UNIVERSE_TRADING_DATE (YYYY-MM-DD) or LESTRADE_EMBED_TRADING_DATES "
        "(comma-separated) for embed scope."
    )
