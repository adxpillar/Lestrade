"""
Parse issuer-level fields from SEC ``company submissions`` JSON
(``GET https://data.sec.gov/submissions/CIK##########.json``).

Used for canonical SIC / entity metadata (not Yahoo).
"""

from __future__ import annotations

from typing import Any


def _clean_str(x: Any) -> str | None:
    if x is None:
        return None
    s = str(x).strip()
    return s or None


def _sic_code_str(x: Any) -> str | None:
    if x is None or x == "":
        return None
    if isinstance(x, (int, float)) and x == x:  # not NaN
        return str(int(x))
    s = str(x).strip()
    return s or None


def _str_list(x: Any) -> list[str] | None:
    if not isinstance(x, list):
        return None
    out = [str(v).strip() for v in x if v is not None and str(v).strip()]
    return out or None


def issuer_metadata_from_submissions(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Map submissions JSON top-level fields to a flat dict for DB columns.

    SEC schema evolves; unknown keys are ignored. ``tickers`` / ``exchanges`` are
    parallel string arrays when present.
    """
    tickers = _str_list(payload.get("tickers"))
    exchanges = _str_list(payload.get("exchanges"))
    return {
        "entity_name": _clean_str(payload.get("name")),
        "entity_type": _clean_str(payload.get("entityType")),
        "sic_code": _sic_code_str(payload.get("sic")),
        "sic_description": _clean_str(payload.get("sicDescription")),
        "state_of_incorporation": _clean_str(payload.get("stateOfIncorporation")),
        "fiscal_year_end": _clean_str(payload.get("fiscalYearEnd")),
        "tickers": tickers,
        "exchanges": exchanges,
    }
