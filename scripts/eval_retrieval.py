#!/usr/bin/env python3
"""
Smoke-test retrieval against both Chroma collections (MiniLM + Voyage).

Usage (from repo root, with [embed] installed and env set):

  export LESTRADE_CHROMA_PERSIST_DIR=/path/to/chroma
  export VOYAGE_API_KEY=...
  python scripts/eval_retrieval.py
"""

from __future__ import annotations

import os
import sys

# Fixed eval questions — extend as you add real filing data.
EVAL_QUESTIONS = [
    "CEO open market stock purchase",
    "insider sale of shares",
    "Form 4 amendment",
    "option exercise by director",
]


def _query_collection(model_id: str, question: str, *, n: int = 3) -> None:
    from lestrade_embed.chroma_store import ChromaTransactionStore
    from lestrade_embed.config import get_model_spec
    spec = get_model_spec(model_id)
    store = ChromaTransactionStore(spec)
    if spec.provider == "voyage":
        import voyageai

        key = (os.environ.get("VOYAGE_API_KEY") or "").strip()
        if not key:
            raise SystemExit("VOYAGE_API_KEY required for Voyage queries")
        client = voyageai.Client(api_key=key)
        qvec = client.embed([question], model=spec.model_id, input_type="query").embeddings[0]
    else:
        from sentence_transformers import SentenceTransformer

        model = SentenceTransformer(spec.model_id)
        qvec = model.encode([question], normalize_embeddings=True)[0].tolist()

    res = store.query(qvec, n_results=n)
    print(f"\n=== {model_id} / {question!r} ===")
    docs = (res.get("documents") or [[]])[0]
    metas = (res.get("metadatas") or [[]])[0]
    dists = (res.get("distances") or [[]])[0]
    for i, doc in enumerate(docs):
        meta = metas[i] if i < len(metas) else {}
        dist = dists[i] if i < len(dists) else None
        acc = meta.get("accession_number", "?")
        tidx = meta.get("transaction_index", "?")
        print(f"  [{i+1}] dist={dist:.4f} id={acc}:{tidx}")
        print(f"      {doc[:200]}..." if len(doc) > 200 else f"      {doc}")


def main() -> int:
    if not (os.environ.get("LESTRADE_CHROMA_PERSIST_DIR") or "").strip():
        print("Set LESTRADE_CHROMA_PERSIST_DIR", file=sys.stderr)
        return 1
    for q in EVAL_QUESTIONS:
        _query_collection("all-MiniLM-L6-v2", q)
        _query_collection("voyage-finance-2", q)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
