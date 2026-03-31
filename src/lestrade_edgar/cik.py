from __future__ import annotations


def pad_cik(cik: str | int) -> str:
    """Normalize CIK to a 10-character zero-padded string (DATA_CONTRACT)."""
    s = str(cik).strip()
    if not s.isdigit():
        raise ValueError(f"CIK must be numeric, got {cik!r}")
    if len(s) > 10:
        raise ValueError(f"CIK too long: {cik!r}")
    return s.zfill(10)


def cik_path_segment(cik_padded: str) -> str:
    """SEC archive path segment: CIK without leading zeros."""
    seg = cik_padded.lstrip("0")
    return seg if seg else "0"


def accession_nodash(accession_number: str) -> str:
    return accession_number.replace("-", "")
