from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from lestrade_embed.chroma_store import ChromaTransactionStore
from lestrade_embed.config import get_model_spec
from lestrade_rag.citations import accession_to_edgar_index_url
from lestrade_rag.config import embed_model_id, n_results as default_n_results


@lru_cache(maxsize=1)
def _minilm_model(model_id: str):
    from sentence_transformers import SentenceTransformer

    return SentenceTransformer(model_id)


@dataclass(frozen=True)
class RetrievedChunk:
    chunk_id: str
    text: str
    distance: float | None
    metadata: dict[str, Any]
    edgar_url: str | None


def _build_where(
    *,
    ticker: str | None,
    acquired_disposed: str | None,
    universe_group: str | None,
) -> dict[str, Any] | None:
    clauses: list[dict[str, Any]] = []
    if ticker:
        clauses.append({"issuer_ticker": ticker.strip().upper()})
    if acquired_disposed:
        code = acquired_disposed.strip().upper()
        if code in ("P", "A", "D", "S"):
            # Form 4 uses A/D; some templates may store P/S — accept both in UI.
            if code == "P":
                code = "A"
            if code == "S":
                code = "D"
            clauses.append({"acquired_disposed_code": code})
    if universe_group in ("high", "low"):
        clauses.append({"universe_group": universe_group})
    if not clauses:
        return None
    if len(clauses) == 1:
        return clauses[0]
    return {"$and": clauses}


def retrieve(
    question: str,
    *,
    ticker: str | None = None,
    acquired_disposed: str | None = None,
    universe_group: str | None = None,
    n_results: int | None = None,
    model_id: str | None = None,
    persist_directory: str | None = None,
) -> list[RetrievedChunk]:
    """Embed ``question`` with MiniLM and query the Chroma collection."""
    q = (question or "").strip()
    if not q:
        return []

    mid = model_id or embed_model_id()
    spec = get_model_spec(mid)
    if spec.provider != "minilm":
        raise ValueError(
            f"RAG demo uses MiniLM only; got provider={spec.provider!r} for {mid!r}"
        )

    model = _minilm_model(spec.model_id)
    qvec = model.encode([q], normalize_embeddings=True)[0].tolist()
    store = ChromaTransactionStore(spec, persist_directory=persist_directory)
    where = _build_where(
        ticker=ticker,
        acquired_disposed=acquired_disposed,
        universe_group=universe_group,
    )
    k = n_results if n_results is not None else default_n_results()
    raw = store.query(qvec, n_results=k, where=where)

    docs = (raw.get("documents") or [[]])[0]
    metas = (raw.get("metadatas") or [[]])[0]
    dists = (raw.get("distances") or [[]])[0]
    ids = (raw.get("ids") or [[]])[0]

    out: list[RetrievedChunk] = []
    for i, doc in enumerate(docs):
        meta = metas[i] if i < len(metas) else {}
        dist = dists[i] if i < len(dists) else None
        cid = ids[i] if i < len(ids) else f"unknown:{i}"
        acc = str(meta.get("accession_number") or "")
        cik = meta.get("issuer_cik")
        cik_s = str(cik) if cik is not None else None
        out.append(
            RetrievedChunk(
                chunk_id=str(cid),
                text=str(doc or ""),
                distance=float(dist) if dist is not None else None,
                metadata=dict(meta),
                edgar_url=accession_to_edgar_index_url(acc, cik_s),
            )
        )
    return out
