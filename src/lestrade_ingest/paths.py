from __future__ import annotations

import re


_ACCESSION_DIR = re.compile(r"/(\d{18})/")


def accession_from_archive_path(archive_path: str) -> str | None:
    """
    Extract SEC accession (with hyphens) from a master.idx ``File Name`` path.

    Paths look like ``edgar/data/320193/000032019325000042/xslF345X05/doc.xml``.
    """
    s = archive_path.strip().replace("\\", "/")
    m = _ACCESSION_DIR.search("/" + s.strip("/") + "/")
    if not m:
        return None
    nodash = m.group(1)
    if len(nodash) != 18 or not nodash.isdigit():
        return None
    return f"{nodash[:10]}-{nodash[10:12]}-{nodash[12:]}"


def primary_document_from_archive_path(archive_path: str) -> str | None:
    """Relative path under the accession directory (SEC primary document path)."""
    s = archive_path.strip().replace("\\", "/")
    m = _ACCESSION_DIR.search("/" + s.strip("/") + "/")
    if not m:
        return None
    nodash = m.group(1)
    idx = s.find(nodash)
    if idx < 0:
        return None
    tail = s[idx + len(nodash) :].lstrip("/")
    return tail or None
