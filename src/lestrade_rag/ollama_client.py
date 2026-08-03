from __future__ import annotations

import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from lestrade_rag.config import ollama_base_url, ollama_model


class OllamaError(RuntimeError):
    pass


def chat(
    messages: list[dict[str, str]],
    *,
    model: str | None = None,
    base_url: str | None = None,
    timeout_s: float = 120.0,
) -> str:
    """
    Call Ollama ``/api/chat`` (non-streaming).

    ``messages`` items: ``{"role": "system"|"user"|"assistant", "content": "..."}``.
    """
    url = (base_url or ollama_base_url()).rstrip("/") + "/api/chat"
    payload: dict[str, Any] = {
        "model": model or ollama_model(),
        "messages": messages,
        "stream": False,
        "options": {
            "temperature": 0.2,
        },
    }
    body = json.dumps(payload).encode("utf-8")
    req = Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(req, timeout=timeout_s) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")[:500]
        raise OllamaError(f"Ollama HTTP {e.code}: {detail}") from e
    except URLError as e:
        raise OllamaError(
            f"Cannot reach Ollama at {url}. Is it running? ({e.reason})"
        ) from e

    message = data.get("message") or {}
    content = (message.get("content") or "").strip()
    if not content:
        raise OllamaError(f"Ollama returned empty content: {data!r}"[:500])
    return content


def ping(*, base_url: str | None = None, timeout_s: float = 5.0) -> bool:
    url = (base_url or ollama_base_url()).rstrip("/") + "/api/tags"
    req = Request(url, method="GET")
    try:
        with urlopen(req, timeout=timeout_s) as resp:
            return 200 <= getattr(resp, "status", 200) < 300
    except Exception:
        return False
