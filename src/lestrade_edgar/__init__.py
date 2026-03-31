"""SEC EDGAR HTTP client: discovery, fetch, rate limiting, retries."""

from lestrade_edgar.client import EdgarClient
from lestrade_edgar.exceptions import EdgarError, EdgarRequestError
from lestrade_edgar.models import (
    FetchResult,
    Form4FilingRef,
    MasterIndexRow,
    StoredRawXml,
)

__all__ = [
    "EdgarClient",
    "EdgarError",
    "EdgarRequestError",
    "FetchResult",
    "Form4FilingRef",
    "MasterIndexRow",
    "StoredRawXml",
]
