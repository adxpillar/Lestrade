from __future__ import annotations


class EdgarError(Exception):
    """Base for EDGAR client errors."""


class EdgarRequestError(EdgarError):
    """HTTP or transport failure after retries."""

    def __init__(self, message: str, *, url: str | None = None, status_code: int | None = None) -> None:
        super().__init__(message)
        self.url = url
        self.status_code = status_code
