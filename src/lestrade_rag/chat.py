from __future__ import annotations

from dataclasses import dataclass

from lestrade_rag.ollama_client import chat as ollama_chat
from lestrade_rag.retrieve import RetrievedChunk, retrieve

SYSTEM_PROMPT = """You are Lestrade, a research assistant for SEC Form 4 insider trading filings.
Answer ONLY using the retrieved filing excerpts below. Do not invent filings, amounts, or people.
When you cite a fact, include the accession number in parentheses.
If the excerpts are insufficient, say so and suggest broadening filters (ticker, buy/sell, date).
This is not investment advice. Be concise and factual.
Distinguish filing date vs transaction date when both appear.
"""


@dataclass(frozen=True)
class RagAnswer:
    answer: str
    chunks: list[RetrievedChunk]


def _format_context(chunks: list[RetrievedChunk]) -> str:
    if not chunks:
        return "(no excerpts retrieved)"
    parts: list[str] = []
    for i, ch in enumerate(chunks, start=1):
        meta = ch.metadata
        header = (
            f"[{i}] accession={meta.get('accession_number')} "
            f"txn_index={meta.get('transaction_index')} "
            f"ticker={meta.get('issuer_ticker')} "
            f"code={meta.get('transaction_code')} "
            f"A/D={meta.get('acquired_disposed_code')}"
        )
        if ch.edgar_url:
            header += f"\nURL: {ch.edgar_url}"
        parts.append(f"{header}\n{ch.text}")
    return "\n\n".join(parts)


def answer_question(
    question: str,
    *,
    ticker: str | None = None,
    acquired_disposed: str | None = None,
    universe_group: str | None = None,
    n_results: int | None = None,
    ollama_model: str | None = None,
    history: list[dict[str, str]] | None = None,
) -> RagAnswer:
    """
    Retrieve MiniLM chunks, then ask Ollama to answer from that context only.
    """
    chunks = retrieve(
        question,
        ticker=ticker,
        acquired_disposed=acquired_disposed,
        universe_group=universe_group,
        n_results=n_results,
    )
    context = _format_context(chunks)
    user_block = (
        f"Filters: ticker={ticker or 'any'}, "
        f"side={acquired_disposed or 'any'}, "
        f"universe_group={universe_group or 'any'}\n\n"
        f"Retrieved excerpts:\n{context}\n\n"
        f"Question: {question}"
    )
    messages: list[dict[str, str]] = [{"role": "system", "content": SYSTEM_PROMPT}]
    if history:
        # Keep a short tail of prior turns for follow-ups (entity resolution).
        messages.extend(history[-6:])
    messages.append({"role": "user", "content": user_block})

    if not chunks:
        return RagAnswer(
            answer=(
                "I could not find matching Form 4 transactions for that question "
                "with the current filters. Try clearing the ticker filter or broadening the question."
            ),
            chunks=[],
        )

    text = ollama_chat(messages, model=ollama_model)
    return RagAnswer(answer=text, chunks=chunks)
