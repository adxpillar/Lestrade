from __future__ import annotations

import os
from typing import Any, Sequence

from lestrade_embed.config import ModelSpec


def chroma_persist_dir() -> str:
    path = (os.environ.get("LESTRADE_CHROMA_PERSIST_DIR") or "").strip()
    if not path:
        raise ValueError(
            "LESTRADE_CHROMA_PERSIST_DIR is required (e.g. /opt/airflow/chroma on Docker volume)"
        )
    return path


def _sanitize_metadata(meta: dict[str, Any]) -> dict[str, str | int | float | bool]:
    out: dict[str, str | int | float | bool] = {}
    for k, v in meta.items():
        if v is None:
            continue
        if isinstance(v, bool):
            out[k] = v
        elif isinstance(v, int) and not isinstance(v, bool):
            out[k] = v
        elif isinstance(v, float):
            out[k] = v
        elif isinstance(v, str):
            out[k] = v
        else:
            out[k] = str(v)
    return out


class ChromaTransactionStore:
    def __init__(self, spec: ModelSpec, *, persist_directory: str | None = None) -> None:
        import chromadb

        self._spec = spec
        path = persist_directory or chroma_persist_dir()
        self._client = chromadb.PersistentClient(path=path)
        self._collection = self._client.get_or_create_collection(
            name=spec.chroma_collection,
            metadata={"hnsw:space": "cosine", "model_id": spec.model_id},
        )

    @property
    def collection_name(self) -> str:
        return self._spec.chroma_collection

    def upsert(
        self,
        *,
        ids: Sequence[str],
        embeddings: Sequence[Sequence[float]],
        documents: Sequence[str],
        metadatas: Sequence[dict[str, Any]],
    ) -> None:
        if not ids:
            return
        self._collection.upsert(
            ids=list(ids),
            embeddings=[list(e) for e in embeddings],
            documents=list(documents),
            metadatas=[_sanitize_metadata(m) for m in metadatas],
        )

    def query(
        self,
        query_embedding: Sequence[float],
        *,
        n_results: int = 5,
        where: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "query_embeddings": [list(query_embedding)],
            "n_results": n_results,
            "include": ["documents", "metadatas", "distances"],
        }
        if where:
            kwargs["where"] = where
        return self._collection.query(**kwargs)
