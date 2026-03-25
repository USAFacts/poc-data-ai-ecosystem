"""Cross-encoder re-ranker for improved relevance scoring.

This module provides a re-ranking capability using cross-encoder models
that directly estimate query-document relevance, producing more accurate
scores than bi-encoder similarity.

Cross-encoders process query and document together, enabling deeper
semantic understanding at the cost of speed (hence used for re-ranking
a small candidate set, not initial retrieval).
"""

import logging
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)

# Model configuration
RERANKER_MODEL = "cross-encoder/ms-marco-MiniLM-L-6-v2"

# Lazy-loaded model instance
_reranker_model = None


def get_reranker_model():
    """Get or initialize the cross-encoder re-ranker model.

    Uses ms-marco-MiniLM-L-6-v2, a fast and accurate cross-encoder
    trained on MS MARCO passage ranking data.

    Returns:
        CrossEncoder model instance
    """
    global _reranker_model

    if _reranker_model is None:
        try:
            from sentence_transformers import CrossEncoder

            logger.info(f"Loading cross-encoder model: {RERANKER_MODEL}")
            _reranker_model = CrossEncoder(RERANKER_MODEL)
            logger.info("Cross-encoder model loaded successfully")
        except ImportError:
            logger.warning(
                "sentence-transformers not installed. "
                "Install with: pip install sentence-transformers"
            )
            return None
        except Exception as e:
            logger.error(f"Failed to load cross-encoder model: {e}")
            return None

    return _reranker_model


def rerank_documents(
    query: str,
    documents: list[dict[str, Any]],
    top_k: int = 5,
) -> list[tuple[dict[str, Any], float]]:
    """Re-rank documents using cross-encoder for accurate relevance scores.

    Args:
        query: The search query
        documents: List of document dicts (must have text content)
        top_k: Number of top documents to return

    Returns:
        List of (document, score) tuples sorted by relevance score (0-1)
    """
    if not documents:
        return []

    model = get_reranker_model()
    if model is None:
        # Fallback: return documents with original scores if available
        logger.warning("Re-ranker not available, returning documents unchanged")
        return [(doc, doc.get("_score", 0.5)) for doc in documents[:top_k]]

    # Build query-document pairs for cross-encoder
    pairs = []
    for doc in documents:
        doc_text = _extract_document_text(doc)
        pairs.append([query, doc_text])

    # Get cross-encoder scores
    try:
        scores = model.predict(pairs)

        # Convert logits to probabilities using sigmoid
        # Cross-encoder outputs raw scores; sigmoid normalizes to 0-1
        probabilities = 1 / (1 + np.exp(-np.array(scores)))

        # Pair documents with scores
        scored_docs = list(zip(documents, probabilities))

        # Sort by score descending
        scored_docs.sort(key=lambda x: x[1], reverse=True)

        return scored_docs[:top_k]

    except Exception as e:
        logger.error(f"Re-ranking failed: {e}")
        return [(doc, doc.get("_score", 0.5)) for doc in documents[:top_k]]


def _extract_document_text(doc: dict[str, Any], max_length: int = 512) -> str:
    """Extract searchable text from document for re-ranking.

    Prioritizes high-signal content: title, summary, key sections.
    Supports both nested enriched-document dicts (metadata.title,
    enrichment.document.summary) and flat Weaviate chunk dicts where
    title, text, summary, agency are top-level keys.

    Args:
        doc: Document dictionary
        max_length: Maximum text length (cross-encoders have token limits)

    Returns:
        Concatenated document text
    """
    parts = []

    # Title (high priority) — try nested metadata first, then flat
    metadata = doc.get("metadata", {})
    title = metadata.get("title", "") or doc.get("title", "")
    if title:
        parts.append(f"Title: {title}")

    # Summary from enrichment (high signal) — try nested first, then flat
    enrichment = doc.get("enrichment", {})
    document_info = enrichment.get("document", {})
    summary = document_info.get("summary", "") or doc.get("summary", "")
    if summary:
        parts.append(f"Summary: {summary}")

    # Key topics
    topics = document_info.get("keyTopics", []) or doc.get("key_topics", [])
    if topics:
        topic_str = ", ".join(str(t) for t in topics[:10])
        parts.append(f"Topics: {topic_str}")

    # Flat text field (Weaviate chunks store content here)
    flat_text = doc.get("text", "")
    if flat_text and flat_text not in (title, summary):
        parts.append(flat_text[:400])

    # First section content (nested docs)
    content = doc.get("content", {})
    sections = content.get("sections", [])
    if sections:
        first_section = sections[0]
        section_content = first_section.get("content", "")[:300]
        if section_content:
            parts.append(section_content)

    # Agency — flat fallback
    agency = doc.get("agency", "")
    if agency and not metadata:
        parts.append(f"Agency: {agency}")

    # Table descriptions
    tables = enrichment.get("tables", [])
    if tables:
        table_descs = [t.get("description", "") for t in tables[:3] if t.get("description")]
        if table_descs:
            parts.append("Tables: " + " | ".join(table_descs))

    text = " ".join(parts)

    # Truncate to max length
    if len(text) > max_length:
        text = text[:max_length]

    return text


def compute_rrf_score(
    semantic_rank: int,
    keyword_rank: int,
    k: int = 60,
) -> float:
    """Compute Reciprocal Rank Fusion score.

    RRF combines rankings from multiple retrieval methods more effectively
    than simple score averaging, as it's rank-based rather than score-based.

    Formula: RRF(d) = sum(1 / (k + rank_i(d))) for each ranking method

    Args:
        semantic_rank: Rank from semantic search (1-indexed, 0 if not retrieved)
        keyword_rank: Rank from keyword search (1-indexed, 0 if not retrieved)
        k: Smoothing constant (default 60, from original RRF paper)

    Returns:
        RRF score (higher is better)
    """
    score = 0.0

    if semantic_rank > 0:
        score += 1.0 / (k + semantic_rank)

    if keyword_rank > 0:
        score += 1.0 / (k + keyword_rank)

    return score


def calibrate_relevance_score(
    reranker_score: float,
    rrf_score: float,
    keyword_match_ratio: float,
) -> float:
    """Calibrate final relevance score to meaningful percentage.

    Combines re-ranker probability with RRF and keyword signals
    to produce a well-calibrated relevance score.

    Args:
        reranker_score: Cross-encoder probability (0-1)
        rrf_score: Reciprocal Rank Fusion score
        keyword_match_ratio: Ratio of matched keywords/entities (0-1)

    Returns:
        Calibrated relevance score (0-1)
    """
    # Normalize RRF score (typical range 0-0.033 for 2 methods with k=60)
    # Max RRF with rank 1 in both: 1/61 + 1/61 = 0.0328
    normalized_rrf = min(rrf_score / 0.033, 1.0)

    # Weight components:
    # - Re-ranker: 50% (most accurate relevance signal)
    # - RRF: 30% (combines retrieval rankings)
    # - Keyword match: 20% (explicit term matching)

    calibrated = (
        0.50 * reranker_score +
        0.30 * normalized_rrf +
        0.20 * keyword_match_ratio
    )

    # Apply slight boost for high-confidence matches
    if reranker_score > 0.8 and keyword_match_ratio > 0.5:
        calibrated = min(calibrated * 1.1, 1.0)

    return round(calibrated, 3)


class ReRanker:
    """High-level re-ranking interface for search results.

    Combines cross-encoder re-ranking with RRF fusion for optimal
    relevance scoring.

    Example:
        >>> reranker = ReRanker()
        >>> results = reranker.rerank(
        ...     query="H-1B visa processing times",
        ...     semantic_results=search_results,
        ...     keyword_results=keyword_results,
        ...     top_k=5
        ... )
    """

    def __init__(self):
        """Initialize the re-ranker."""
        self._model = None

    @property
    def model(self):
        """Lazy-load the cross-encoder model."""
        if self._model is None:
            self._model = get_reranker_model()
        return self._model

    @property
    def is_available(self) -> bool:
        """Check if re-ranker model is available."""
        return self.model is not None

    def rerank(
        self,
        query: str,
        semantic_results: list[tuple[str, float, dict, dict]],
        keyword_scores: dict[str, float],
        top_k: int = 5,
    ) -> list[tuple[dict, float, dict]]:
        """Re-rank search results using cross-encoder and RRF.

        Args:
            query: Search query
            semantic_results: Search results as (doc_id, score, metadata, doc) tuples
            keyword_scores: Dict mapping doc_id to keyword relevance score
            top_k: Number of results to return

        Returns:
            List of (document, calibrated_score, debug_info) tuples
        """
        if not semantic_results:
            return []

        # Build unified candidate set
        candidates = {}
        for rank, (doc_id, sem_score, metadata, doc) in enumerate(semantic_results, 1):
            candidates[doc_id] = {
                "doc": doc,
                "metadata": metadata,
                "semantic_rank": rank,
                "semantic_score": sem_score,
                "keyword_rank": 0,
                "keyword_score": keyword_scores.get(doc_id, 0.0),
            }

        # Add keyword rankings
        sorted_by_keyword = sorted(
            candidates.items(),
            key=lambda x: x[1]["keyword_score"],
            reverse=True
        )
        for rank, (doc_id, _) in enumerate(sorted_by_keyword, 1):
            candidates[doc_id]["keyword_rank"] = rank

        # Compute RRF scores
        for doc_id, info in candidates.items():
            info["rrf_score"] = compute_rrf_score(
                info["semantic_rank"],
                info["keyword_rank"]
            )

        # Re-rank with cross-encoder
        docs_for_rerank = [info["doc"] for info in candidates.values()]

        if self.is_available:
            reranked = rerank_documents(query, docs_for_rerank, top_k=len(docs_for_rerank))

            # Map re-ranker scores back to candidates
            doc_to_rerank_score = {}
            for doc, score in reranked:
                # Use source path as identifier
                source = doc.get("source", {})
                doc_key = f"{source.get('agency', '')}/{source.get('asset', '')}"
                doc_to_rerank_score[doc_key] = score

            for doc_id, info in candidates.items():
                source = info["doc"].get("source", {})
                doc_key = f"{source.get('agency', '')}/{source.get('asset', '')}"
                info["reranker_score"] = doc_to_rerank_score.get(doc_key, 0.5)
        else:
            # Fallback: use semantic score as proxy
            for info in candidates.values():
                info["reranker_score"] = info["semantic_score"]

        # Calculate keyword match ratio
        for info in candidates.values():
            # Estimate match ratio from keyword score (max ~0.67 after normalization)
            info["keyword_match_ratio"] = min(info["keyword_score"] / 0.67, 1.0)

        # Compute calibrated final scores
        results = []
        for doc_id, info in candidates.items():
            calibrated = calibrate_relevance_score(
                info["reranker_score"],
                info["rrf_score"],
                info["keyword_match_ratio"]
            )

            debug_info = {
                "semantic_score": round(info["semantic_score"], 3),
                "keyword_score": round(info["keyword_score"], 3),
                "reranker_score": round(info["reranker_score"], 3),
                "rrf_score": round(info["rrf_score"], 4),
                "semantic_rank": info["semantic_rank"],
                "keyword_rank": info["keyword_rank"],
            }

            results.append((info["doc"], calibrated, debug_info))

        # Sort by calibrated score
        results.sort(key=lambda x: x[1], reverse=True)

        return results[:top_k]
