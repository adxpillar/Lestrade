"""Phase 3 — Form 4 transaction embeddings (Chroma + Postgres bookkeeping)."""

from lestrade_embed.config import MODEL_REGISTRY, ModelSpec, get_model_spec

__all__ = [
    "MODEL_REGISTRY",
    "ModelSpec",
    "get_model_spec",
]
