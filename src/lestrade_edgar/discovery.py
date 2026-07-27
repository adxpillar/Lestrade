from __future__ import annotations

import io
import re
from datetime import date, datetime
from typing import Any, Iterator

from lestrade_edgar.cik import pad_cik
from lestrade_edgar.models import Form4FilingRef, MasterIndexRow

_FORM4 = frozenset({"4", "4/A"})


def submissions_json_url(cik_padded: str) -> str:
    return f"https://data.sec.gov/submissions/CIK{cik_padded}.json"


def daily_master_index_url(d: date) -> str:
    q = _quarter_prefix(d)
    y = d.year
    stamp = d.strftime("%Y%m%d")
    return f"https://www.sec.gov/Archives/edgar/daily-index/{y}/{q}/master{stamp}.idx"


def _quarter_prefix(d: date) -> str:
    m = d.month
    if m <= 3:
        return "QTR1"
    if m <= 6:
        return "QTR2"
    if m <= 9:
        return "QTR3"
    return "QTR4"


def iter_form4_from_submissions(
    payload: dict[str, Any],
    *,
    cik: str | None = None,
) -> Iterator[Form4FilingRef]:
    """
    Yield Form 4 / 4/A rows from a submissions JSON object.

    Uses `filings.recent` parallel arrays. Optional `cik` overrides issuer CIK
    (otherwise taken from payload['cik']).
    """
    if cik is None:
        raw_cik = payload.get("cik")
        if raw_cik is None:
            raise ValueError("submissions JSON missing 'cik'")
        cik_padded = pad_cik(str(raw_cik))
    else:
        cik_padded = pad_cik(cik)

    filings = payload.get("filings") or {}
    recent = filings.get("recent") or {}
    forms = recent.get("form") or []
    dates = recent.get("filingDate") or []
    accs = recent.get("accessionNumber") or []
    primaries = recent.get("primaryDocument") or []

    n = min(len(forms), len(dates), len(accs), len(primaries))
    for i in range(n):
        form = forms[i]
        if form not in _FORM4:
            continue
        fd = _parse_iso_date(dates[i], field="filingDate")
        acc = str(accs[i])
        doc = str(primaries[i])
        yield Form4FilingRef(
            cik=cik_padded,
            accession_number=acc,
            filing_date=fd,
            primary_document=doc,
            form=form,  # type: ignore[arg-type]
        )


def parse_master_idx_form4(content: str) -> list[MasterIndexRow]:
    """
    Parse a master .idx body; return rows where form type is 4 or 4/A.

    Handles pipe-delimited lines (current daily index) and legacy whitespace runs.
    """
    out: list[MasterIndexRow] = []
    for line in content.splitlines():
        s = line.strip()
        if not s or s.startswith("---") or s.startswith("Copyright"):
            continue
        lower = s.lower()
        if lower.startswith("description:") or lower.startswith("line format:"):
            continue

        parts = _split_idx_line(s)
        if len(parts) < 5:
            continue
        cik_raw, name, form, date_raw, path = parts[0], parts[1], parts[2], parts[3], parts[4]
        if form not in _FORM4:
            continue
        try:
            cik_padded = pad_cik(cik_raw)
        except ValueError:
            continue
        try:
            filed = _parse_idx_date(date_raw)
        except ValueError:
            continue
        out.append(
            MasterIndexRow(
                cik=cik_padded,
                company_name=name.strip(),
                form_type=form,
                date_filed=filed,
                archive_path=path.strip(),
            )
        )
    return out


_PIPE_SPLIT = re.compile(r"\|")


def _split_idx_line(line: str) -> list[str]:
    if "|" in line:
        return [p.strip() for p in _PIPE_SPLIT.split(line)]
    return _split_whitespace_runs(line)


def _split_whitespace_runs(line: str) -> list[str]:
    """Legacy: CIK, name (may contain commas), form, date, filename — approximate split."""
    parts = line.split()
    if len(parts) < 5:
        return []
    cik = parts[0]
    date_idx = None
    for j in range(3, len(parts)):
        if _looks_like_idx_date(parts[j]):
            date_idx = j
            break
    if date_idx is None or date_idx < 3:
        return []
    form = parts[date_idx - 1]
    name = " ".join(parts[1 : date_idx - 1])
    date_raw = parts[date_idx]
    path = " ".join(parts[date_idx + 1 :])
    return [cik, name, form, date_raw, path]


def _looks_like_idx_date(s: str) -> bool:
    if len(s) != 10 or s[4] != "-" or s[7] != "-":
        return False
    return s[:4].isdigit() and s[5:7].isdigit() and s[8:10].isdigit()


def _parse_idx_date(s: str) -> date:
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"unrecognized index date: {s!r}")


def _parse_iso_date(s: str, *, field: str) -> date:
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d").date()
    except ValueError as e:
        raise ValueError(f"bad {field} date: {s!r}") from e


def master_row_to_url(row: MasterIndexRow) -> str:
    """Absolute https URL for an index filename path."""
    p = row.archive_path.lstrip("/")
    return f"https://www.sec.gov/Archives/{p}"


def parse_filing_index_for_primary_xml(html: str) -> str | None:
    """
    Best-effort: from an EDGAR filing index HTML page, find a primary Form 4 XML href.

    Prefer links ending in .xml that look like ownership / Form 4 paths.
    """
    # HREF="/Archives/edgar/data/.../xslF345X05/wf-form4_....xml"
    hrefs = re.findall(r'href="([^"]+\.xml)"', html, flags=re.IGNORECASE)
    if not hrefs:
        return None

    def _is_xsl_wrapper(h: str) -> bool:
        low = h.lower()
        # Common SEC directory for XSL-rendered "XML" that is actually an HTML landing page.
        return "/xslf345" in low or "xslf345x" in low

    # If any non-XSL XML links exist, prefer them and ignore the XSL wrappers.
    non_xsl = [h for h in hrefs if not _is_xsl_wrapper(h)]
    candidates = non_xsl or hrefs

    def score(h: str) -> tuple[int, int]:
        low = h.lower()
        pri = 0
        # Strongest signals: direct form4 XML filenames / ownership forms.
        if "form4" in low or "form_4" in low or "ownership" in low:
            pri = 3
        elif "doc4" in low:
            pri = 2
        # XSL wrapper links should be last-resort (kept only if nothing else exists).
        if _is_xsl_wrapper(low):
            pri = min(pri, 0)
        return (-pri, len(h))

    hrefs_sorted = sorted(candidates, key=score)
    pick = hrefs_sorted[0]
    if pick.startswith("http://") or pick.startswith("https://"):
        return pick
    if pick.startswith("/"):
        return f"https://www.sec.gov{pick}"
    return f"https://www.sec.gov/Archives/{pick.lstrip('/')}"


def read_filing_index_text(body: bytes) -> str:
    """Decode filing index HTML; utf-8 with replacement."""
    return io.TextIOWrapper(io.BytesIO(body), encoding="utf-8", errors="replace").read()
