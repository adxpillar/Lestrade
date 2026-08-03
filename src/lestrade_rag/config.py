from __future__ import annotations

import os


DEFAULT_EMBED_MODEL = "all-MiniLM-L6-v2"
DEFAULT_OLLAMA_HOST = "http://127.0.0.1:11434"
DEFAULT_OLLAMA_MODEL = "llama3.2"
DEFAULT_N_RESULTS = 6


def chroma_persist_dir() -> str:
    from lestrade_embed.chroma_store import chroma_persist_dir as _embed_dir

    return _embed_dir()


def ollama_base_url() -> str:
    return (os.environ.get("LESTRADE_OLLAMA_BASE_URL") or DEFAULT_OLLAMA_HOST).strip().rstrip("/")


def ollama_model() -> str:
    return (os.environ.get("LESTRADE_OLLAMA_MODEL") or DEFAULT_OLLAMA_MODEL).strip()


def embed_model_id() -> str:
    return (os.environ.get("LESTRADE_RAG_EMBED_MODEL") or DEFAULT_EMBED_MODEL).strip()


def n_results() -> int:
    raw = (os.environ.get("LESTRADE_RAG_N_RESULTS") or str(DEFAULT_N_RESULTS)).strip()
    try:
        n = int(raw)
    except ValueError:
        n = DEFAULT_N_RESULTS
    return max(1, min(n, 20))
