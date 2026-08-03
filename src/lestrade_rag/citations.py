from __future__ import annotations


def accession_to_edgar_index_url(accession_number: str, issuer_cik: str | None) -> str | None:
    """
    Build an EDGAR filing index URL when CIK is known.

    Path uses zero-padded CIK and accession without dashes.
    """
    acc = (accession_number or "").strip()
    if not acc:
        return None
    digits = "".join(c for c in (issuer_cik or "") if c.isdigit())
    if not digits:
        # Fallback search page (still useful without CIK).
        return f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=&type=4&dateb=&owner=include&count=40&search_text={acc}"
    cik_int = str(int(digits))  # drop leading zeros for path segment
    acc_nodash = acc.replace("-", "")
    return f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_nodash}/{acc}-index.htm"
