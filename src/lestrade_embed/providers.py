from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Sequence

from lestrade_embed.config import ModelSpec, get_model_spec


class EmbeddingProvider(ABC):
    @abstractmethod
    def embed_documents(self, texts: Sequence[str]) -> list[list[float]]:
        ...


class MiniLMProvider(EmbeddingProvider):
    def __init__(self, model_id: str = "all-MiniLM-L6-v2") -> None:
        from sentence_transformers import SentenceTransformer

        self._model_id = model_id
        self._model = SentenceTransformer(model_id)

    def embed_documents(self, texts: Sequence[str]) -> list[list[float]]:
        if not texts:
            return []
        vecs = self._model.encode(list(texts), normalize_embeddings=True)
        return [v.tolist() for v in vecs]


class VoyageProvider(EmbeddingProvider):
    def __init__(self, model_id: str = "voyage-finance-2", *, api_key: str | None = None) -> None:
        import os

        import voyageai

        key = (api_key or os.environ.get("VOYAGE_API_KEY") or "").strip()
        if not key:
            raise ValueError("VOYAGE_API_KEY is required for voyage-finance-2 embeddings")
        self._model_id = model_id
        self._client = voyageai.Client(api_key=key)

    def embed_documents(self, texts: Sequence[str]) -> list[list[float]]:
        if not texts:
            return []
        result = self._client.embed(
            list(texts),
            model=self._model_id,
            input_type="document",
        )
        return [list(v) for v in result.embeddings]


def provider_for_spec(spec: ModelSpec) -> EmbeddingProvider:
    if spec.provider == "minilm":
        return MiniLMProvider(spec.model_id)
    if spec.provider == "voyage":
        return VoyageProvider(spec.model_id)
    raise ValueError(f"Unknown provider {spec.provider!r}")


def embed_documents(model_id: str, texts: Sequence[str]) -> list[list[float]]:
    spec = get_model_spec(model_id)
    return provider_for_spec(spec).embed_documents(texts)
