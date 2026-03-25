"""Embedding service for generating document embeddings.

Uses sentence-transformers with all-MiniLM-L6-v2 model (384 dimensions).
Embeddings are stored in enriched documents for fast retrieval at search time.
"""

from typing import Any

import os
import threading

from logging_manager import get_logger

# Disable HuggingFace tokenizers parallelism. We encode single texts,
# so the Rust thread pool is unnecessary. Without this, loky spawns
# worker processes whose semaphores leak at shutdown on macOS.
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")

logger = get_logger(__name__)

# Lazy-loaded model instance.
# The encode lock serializes all encode() calls because the HuggingFace
# Rust tokenizer is not thread-safe ("Already borrowed" panic).
_model = None
_model_lock = threading.Lock()
_encode_lock = threading.Lock()

# Model choice: all-MiniLM-L6-v2 is small (80MB), fast, and good quality
# 384 dimensions, great for semantic search
MODEL_NAME = "all-MiniLM-L6-v2"
EMBEDDING_DIMENSION = 384


def _patch_ssl_for_huggingface():
    """Patch urllib3/requests to use the system certificate store.

    sentence-transformers downloads models from HuggingFace via requests/urllib3,
    which doesn't use truststore by default on macOS Homebrew Python.
    """
    try:
        import truststore
        truststore.inject_into_ssl()
        logger.debug("[embeddings] Injected truststore into SSL")
    except ImportError:
        pass
    except Exception:
        pass


def _get_model():
    """Get the sentence-transformers model (lazy loaded, thread-safe)."""
    global _model
    if _model is not None:
        return _model

    with _model_lock:
        # Double-check after acquiring lock
        if _model is not None:
            return _model

        _patch_ssl_for_huggingface()
        try:
            from sentence_transformers import SentenceTransformer
            logger.info(f"[embeddings] Loading model: {MODEL_NAME}")
            _model = SentenceTransformer(MODEL_NAME)
            logger.info("[embeddings] Model loaded successfully")
        except ImportError:
            logger.warning(
                "[embeddings] sentence-transformers not installed. "
                "Install with: uv pip install sentence-transformers"
            )
            return None
        except Exception as e:
            logger.error(f"[embeddings] Error loading model: {e}")
            return None
    return _model


def get_embedding(text: str) -> list[float] | None:
    """Get embedding for a single text.

    Args:
        text: Text to embed

    Returns:
        Embedding vector as list of floats (384 dimensions), or None if unavailable
    """
    model = _get_model()
    if model is None:
        return None

    with _encode_lock:
        try:
            embedding = model.encode(text, convert_to_numpy=True)
            return embedding.tolist()
        except Exception as e:
            logger.error(f"[embeddings] Error generating embedding: {e}")
            return None


def build_composite_text_for_embedding(doc: dict[str, Any]) -> str:
    """Build composite text from enriched document for embedding.

    Combines multiple semantic signals:
    - Document title
    - Summary
    - Key topics
    - Entity names
    - Section summaries
    - Table descriptions
    - Example queries (great for semantic matching)

    Args:
        doc: Enriched document dictionary

    Returns:
        Composite text optimized for embedding
    """
    parts = []

    # Title from metadata
    metadata = doc.get("metadata", {})
    title = metadata.get("title", "")
    if title:
        parts.append(f"Title: {title}")

    # Summary from enrichment
    enrichment = doc.get("enrichment", {})
    document_enrichment = enrichment.get("document", {})

    summary = document_enrichment.get("summary", "")
    if summary:
        parts.append(f"Summary: {summary}")

    # Key topics
    topics = document_enrichment.get("keyTopics", [])
    if topics:
        parts.append(f"Topics: {', '.join(topics)}")

    # Entity names (prefer canonical names)
    entities = document_enrichment.get("entities", [])
    entity_names = []
    for entity in entities:
        canonical = entity.get("canonicalName") or entity.get("name", "")
        if canonical:
            entity_names.append(canonical)
    if entity_names:
        parts.append(f"Entities: {', '.join(entity_names[:20])}")  # Limit to 20

    # Example queries (excellent for semantic matching)
    example_queries = document_enrichment.get("exampleQueries", [])
    if example_queries:
        parts.append(f"Questions this document answers: {'; '.join(example_queries[:5])}")

    # Section summaries
    sections = enrichment.get("sections", [])
    section_summaries = []
    for section in sections[:5]:  # Limit to first 5 sections
        section_summary = section.get("summary", "")
        if section_summary:
            section_summaries.append(section_summary)
    if section_summaries:
        parts.append(f"Section summaries: {' '.join(section_summaries)}")

    # Table descriptions
    tables = enrichment.get("tables", [])
    table_descriptions = []
    for table in tables[:5]:  # Limit to first 5 tables
        table_desc = table.get("description", "")
        if table_desc:
            table_descriptions.append(table_desc)
    if table_descriptions:
        parts.append(f"Table contents: {' '.join(table_descriptions)}")

    return "\n".join(parts)
