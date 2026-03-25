"""Embedding service for generating document embeddings.

Uses sentence-transformers with all-MiniLM-L6-v2 model (384 dimensions).
Embeddings are computed per-chunk and stored in Weaviate for hybrid search.
"""

import logging
import os
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)

# Lazy-loaded model instance
_model = None
_model_load_attempted = False

# Model choice: all-MiniLM-L6-v2 is small (80MB), fast, and good quality
# 384 dimensions, great for semantic search
MODEL_NAME = "all-MiniLM-L6-v2"
EMBEDDING_DIMENSION = 384


def _get_model():
    """Get the sentence-transformers model (lazy loaded).

    Returns None if the model cannot be loaded (e.g. SSL issues
    prevent downloading from HuggingFace). Callers must handle None.
    """
    global _model, _model_load_attempted
    if _model is not None:
        return _model
    if _model_load_attempted:
        return None
    _model_load_attempted = True
    try:
        from sentence_transformers import SentenceTransformer

        logger.info(f"[embeddings] Loading model: {MODEL_NAME}")
        # Limit HF hub retries to avoid blocking startup
        os.environ.setdefault("HF_HUB_DOWNLOAD_TIMEOUT", "10")
        _model = SentenceTransformer(MODEL_NAME)
        logger.info("[embeddings] Model loaded successfully")
    except Exception as e:
        logger.warning(
            "[embeddings] Could not load embedding model: %s. "
            "Semantic search will be unavailable until the model can be downloaded.",
            e,
        )
        _model = None
    return _model


def get_embedding(text: str) -> np.ndarray | None:
    """Get embedding for a single text.

    Args:
        text: Text to embed

    Returns:
        Embedding vector as numpy array, or None if embedding fails
    """
    try:
        model = _get_model()
        embedding = model.encode(text, convert_to_numpy=True)
        return embedding
    except Exception as e:
        logger.error(f"[embeddings] Error generating embedding: {e}")
        return None


def get_embeddings_batch(texts: list[str]) -> list[np.ndarray] | None:
    """Get embeddings for multiple texts.

    Args:
        texts: List of texts to embed

    Returns:
        List of embedding vectors, or None if embedding fails
    """
    if not texts:
        return []

    try:
        model = _get_model()
        embeddings = model.encode(texts, convert_to_numpy=True, show_progress_bar=False)
        return list(embeddings)
    except Exception as e:
        logger.error(f"[embeddings] Error generating batch embeddings: {e}")
        return None


def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """Calculate cosine similarity between two vectors."""
    dot_product = np.dot(a, b)
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)

    if norm_a == 0 or norm_b == 0:
        return 0.0

    return float(dot_product / (norm_a * norm_b))


def build_composite_text_for_embedding(doc: dict[str, Any]) -> str:
    """Build composite text from enriched document for embedding.

    Combines multiple semantic signals:
    - Document title
    - Summary
    - Key topics
    - Entity names
    - Section summaries
    - Table descriptions
    """
    parts = []

    metadata = doc.get("metadata", {})
    title = metadata.get("title", "")
    if title:
        parts.append(f"Title: {title}")

    enrichment = doc.get("enrichment", {})
    document_enrichment = enrichment.get("document", {})

    summary = document_enrichment.get("summary", "")
    if summary:
        parts.append(f"Summary: {summary}")

    topics = document_enrichment.get("keyTopics", [])
    if topics:
        parts.append(f"Topics: {', '.join(topics)}")

    entities = document_enrichment.get("entities", [])
    entity_names = []
    for entity in entities:
        canonical = entity.get("canonicalName") or entity.get("name", "")
        if canonical:
            entity_names.append(canonical)
    if entity_names:
        parts.append(f"Entities: {', '.join(entity_names[:20])}")

    example_queries = document_enrichment.get("exampleQueries", [])
    if example_queries:
        parts.append(f"Questions this document answers: {'; '.join(example_queries[:5])}")

    sections = enrichment.get("sections", [])
    section_summaries = []
    for section in sections[:5]:
        section_summary = section.get("summary", "")
        if section_summary:
            section_summaries.append(section_summary)
    if section_summaries:
        parts.append(f"Section summaries: {' '.join(section_summaries)}")

    tables = enrichment.get("tables", [])
    table_descriptions = []
    for table in tables[:5]:
        table_desc = table.get("description", "")
        if table_desc:
            table_descriptions.append(table_desc)
    if table_descriptions:
        parts.append(f"Table contents: {' '.join(table_descriptions)}")

    return "\n".join(parts)
