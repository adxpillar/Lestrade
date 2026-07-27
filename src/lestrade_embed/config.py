from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ModelSpec:
    model_id: str
    chroma_collection: str
    dimension: int
    provider: str  # "minilm" | "voyage"


MODEL_REGISTRY: dict[str, ModelSpec] = {
    "all-MiniLM-L6-v2": ModelSpec(
        model_id="all-MiniLM-L6-v2",
        chroma_collection="lestrade_txn_minilm",
        dimension=384,
        provider="minilm",
    ),
    "voyage-finance-2": ModelSpec(
        model_id="voyage-finance-2",
        chroma_collection="lestrade_txn_voyage",
        dimension=1024,
        provider="voyage",
    ),
}

TEMPLATE_VERSION = "1"


def get_model_spec(model_id: str) -> ModelSpec:
    spec = MODEL_REGISTRY.get(model_id)
    if spec is None:
        known = ", ".join(sorted(MODEL_REGISTRY))
        raise ValueError(f"Unknown model_id {model_id!r}; known: {known}")
    return spec
