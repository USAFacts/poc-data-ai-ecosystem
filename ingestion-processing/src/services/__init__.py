"""Services for pipeline processing."""

from services.embeddings import get_embedding, build_composite_text_for_embedding

__all__ = ["get_embedding", "build_composite_text_for_embedding"]
