"""Phase 4 — MiniLM retrieval + Ollama chat (local, low cost)."""

from lestrade_rag.chat import answer_question
from lestrade_rag.retrieve import retrieve

__all__ = ["answer_question", "retrieve"]
