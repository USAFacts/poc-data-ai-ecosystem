"""Search API routes for Q&A assistant queries using Anthropic Claude."""

import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import anthropic
import httpx
from dotenv import load_dotenv
from fastapi import APIRouter
from pydantic import BaseModel

from src.api.deps import DBSession, Storage
from src.models.domain import AgencyModel, AssetModel
from src.services.embeddings import (
    get_embedding,
    cosine_similarity,
    build_composite_text_for_embedding,
)
from src.services.reranker import ReRanker, calibrate_relevance_score, rerank_documents

# Global re-ranker instance (lazy-loaded)
_reranker: ReRanker | None = None


def get_reranker() -> ReRanker:
    """Get or create the re-ranker instance."""
    global _reranker
    if _reranker is None:
        _reranker = ReRanker()
    return _reranker

# Ensure .env is loaded (fallback if main.py didn't load it)
env_path = Path(__file__).parent.parent.parent.parent / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path, override=True)
    print(f"[search.py] Loaded .env from {env_path}")

logger = logging.getLogger(__name__)

router = APIRouter()

# Initialize Anthropic client (will be lazily initialized if not available at startup)
_anthropic_client: anthropic.Anthropic | None = None


def get_anthropic_client() -> anthropic.Anthropic | None:
    """Get the Anthropic client, initializing lazily if needed.

    Supports Azure AI Foundry via ANTHROPIC_BASE_URL env var.
    """
    global _anthropic_client
    if _anthropic_client is None:
        api_key = os.getenv("ANTHROPIC_API_KEY")
        base_url = os.getenv("ANTHROPIC_BASE_URL")
        print(f"[DEBUG search.py] ANTHROPIC_API_KEY exists: {bool(api_key)}")
        if base_url:
            print(f"[DEBUG search.py] Using custom base URL: {base_url}")
        if api_key:
            print(f"[DEBUG search.py] API key prefix: {api_key[:20]}...")
            try:
                client_kwargs = {"api_key": api_key}
                if base_url:
                    client_kwargs["base_url"] = base_url
                    # Use a custom httpx client that skips SSL verification
                    # for Azure AI Foundry endpoints behind corporate proxies
                    client_kwargs["http_client"] = httpx.Client(verify=False)
                _anthropic_client = anthropic.Anthropic(**client_kwargs)
                print("[DEBUG search.py] Anthropic client initialized successfully")
            except Exception as e:
                print(f"[DEBUG search.py] Error initializing Anthropic client: {e}")
                logger.error(f"Error initializing Anthropic client: {e}")
        else:
            print("[DEBUG search.py] ANTHROPIC_API_KEY not found in environment")
            logger.warning("ANTHROPIC_API_KEY not found in environment")
    return _anthropic_client


class ChartSpec(BaseModel):
    """Specification for a chart to render in the frontend."""
    chart_type: str  # "bar", "line", "pie"
    title: str
    x_label: str = ""
    y_label: str = ""
    data: list[dict[str, Any]]


class QueryRequest(BaseModel):
    """Request model for search query."""
    query: str
    mode: str | None = None  # "weaviate_only" or "weaviate_graph"


class EntityMatch(BaseModel):
    """An entity found in the query."""
    text: str
    type: str
    normalized: str


class QueryDecomposition(BaseModel):
    """Decomposed query with identified entities and intent."""
    original_query: str
    entities: list[EntityMatch]
    keywords: list[str]
    intent: str


class DocumentReference(BaseModel):
    """Reference to a source document."""
    document_id: str
    document_title: str
    asset_name: str
    agency_name: str
    agency_id: str
    section: str | None = None
    section_summary: str | None = None
    relevance_score: float
    snippet: str
    # Attribution fields
    original_url: str | None = None
    source_url: str | None = None
    page_count: int | None = None
    page_number: int | None = None
    sheet_name: str | None = None
    file_format: str | None = None
    # Graph context fields
    related_entities: list[str] | None = None
    related_documents: list[str] | None = None
    graph_context: str | None = None


class UsageMetrics(BaseModel):
    """Token usage and data volume metrics."""
    # Token usage
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    context_window_used_percent: float = 0.0  # Percentage of context window used
    # Data volume
    documents_searched: int = 0
    documents_returned: int = 0
    data_volume_bytes: int = 0  # Total bytes of document content processed
    data_volume_display: str = ""  # Human-readable format (KB, MB, etc.)


class AnswerMetrics(BaseModel):
    """Quality metrics for a generated answer."""
    sts: float = 0.0   # Source Traceability Score
    nvs: float = 0.0   # Numerical Verification Score
    hds: int = 0        # Hallucination Detection Score (count of flags)
    cscs: float = 1.0   # Cross-Store Consistency Score


class SearchResult(BaseModel):
    """Search result with attribution."""
    query_decomposition: QueryDecomposition
    documents: list[DocumentReference]
    answer: str
    brief_answer: str = ""  # One-line summary for quick display
    confidence: float
    usage: UsageMetrics | None = None
    charts: list[ChartSpec] = []
    claude_used: bool = False  # Whether Claude API was used successfully
    metrics: AnswerMetrics | None = None


def format_data_volume(bytes_size: int) -> str:
    """Format bytes into human-readable string."""
    if bytes_size < 1024:
        return f"{bytes_size} B"
    elif bytes_size < 1024 * 1024:
        return f"{bytes_size / 1024:.1f} KB"
    elif bytes_size < 1024 * 1024 * 1024:
        return f"{bytes_size / (1024 * 1024):.2f} MB"
    else:
        return f"{bytes_size / (1024 * 1024 * 1024):.2f} GB"


# Claude Sonnet context window size (200K tokens)
CLAUDE_CONTEXT_WINDOW = 200000


def decompose_query_with_claude(query: str) -> tuple[QueryDecomposition, dict]:
    """Use Claude to decompose the query and extract entities.

    Returns tuple of (decomposition, token_usage_dict)
    """
    token_usage = {"input_tokens": 0, "output_tokens": 0}

    client = get_anthropic_client()
    if not client:
        return decompose_query_fallback(query), token_usage

    # Fetch canonical entity names from Neo4j for better entity resolution
    canonical_hint = ""
    try:
        from src.services.neo4j_client import get_all_entity_names
        canonical_entities = get_all_entity_names()
        if canonical_entities:
            sample = canonical_entities[:50]
            canonical_hint = "\n\nKnown canonical entity names (use these exact forms when applicable):\n" + ", ".join(
                f"{e['name']} ({e['type']})" for e in sample if e.get("name")
            )
    except Exception:
        canonical_entities = []

    try:
        message = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=1024,
            messages=[
                {
                    "role": "user",
                    "content": f"""Analyze this query about government data and extract structured information.

Query: "{query}"

Return a JSON object with:
1. "entities": Array of objects with "text" (the entity), "type" (one of: form, visa_type, program, agency, fiscal_year, date, metric, location, organization), and "normalized" (standardized form, e.g., "I-130" not "i130")
2. "keywords": Array of important search keywords (excluding stop words)
3. "intent": One of: "factual" (asking for data/numbers), "comparison" (comparing things), "trend" (asking about changes over time), "definition" (asking what something is), "process" (asking how something works)
{canonical_hint}

Return ONLY valid JSON, no other text.

Example for "How many H-1B visas were approved in FY2023?":
{{"entities": [{{"text": "H-1B", "type": "visa_type", "normalized": "H-1B"}}, {{"text": "FY2023", "type": "fiscal_year", "normalized": "FY2023"}}], "keywords": ["visas", "approved", "approvals"], "intent": "factual"}}"""
                }
            ]
        )

        # Capture token usage
        token_usage["input_tokens"] = message.usage.input_tokens
        token_usage["output_tokens"] = message.usage.output_tokens

        response_text = message.content[0].text.strip()
        # Extract JSON from response (handle potential markdown code blocks)
        if "```" in response_text:
            json_match = re.search(r'```(?:json)?\s*(.*?)\s*```', response_text, re.DOTALL)
            if json_match:
                response_text = json_match.group(1)

        data = json.loads(response_text)

        entities = [
            EntityMatch(
                text=e.get("text", ""),
                type=e.get("type", "unknown"),
                normalized=e.get("normalized", e.get("text", ""))
            )
            for e in data.get("entities", [])
        ]

        # Post-process: match entities against canonical names from Neo4j
        try:
            if canonical_entities:
                canonical_map = {e["name"].lower(): e["name"] for e in canonical_entities if e.get("name")}
                for entity in entities:
                    normalized = entity.normalized.lower()
                    # Exact match
                    if normalized in canonical_map:
                        entity.normalized = canonical_map[normalized]
                    else:
                        # Partial match
                        for canon_lower, canon_name in canonical_map.items():
                            if normalized in canon_lower or canon_lower in normalized:
                                entity.normalized = canon_name
                                break
        except Exception:
            pass

        return QueryDecomposition(
            original_query=query,
            entities=entities,
            keywords=data.get("keywords", [])[:10],
            intent=data.get("intent", "factual")
        ), token_usage
    except Exception as e:
        logger.error(f"Error in decompose_query_with_claude: {e}")
        return decompose_query_fallback(query), {"input_tokens": 0, "output_tokens": 0}


def decompose_query_fallback(query: str) -> QueryDecomposition:
    """Fallback query decomposition using regex patterns."""
    query_lower = query.lower()
    entities: list[EntityMatch] = []

    # Entity patterns - order matters, more specific patterns first
    patterns = {
        "form": r"\b(I-\d{3}[A-Z]?|N-\d{3}|Form\s+\d+)\b",
        "visa_type": r"\b(H-1B|H-2B|H-2A|L-1[AB]?|O-1|EB-[1-5]|F-1|J-1|K-1|TN)\b",
        "program": r"\b(DACA|TPS|VAWA|SIJ|USCIS|DHS|CBP|ICE)\b",
        # FY2025 Q3, FY 2025 Q3, FY25 Q3, fiscal year 2025 Q3, etc.
        "fiscal_year": r"\b(FY\s*\d{2,4}\s*Q[1-4]|FY\s*\d{2,4}|fiscal\s+year\s+\d{4}\s*Q?[1-4]?|Q[1-4]\s*FY\s*\d{2,4}|Q[1-4]\s+\d{4}|\d{4}\s+Q[1-4])\b",
        "date": r"\b(20\d{2})\b",
    }

    for entity_type, pattern in patterns.items():
        for match in re.finditer(pattern, query, re.IGNORECASE):
            entities.append(EntityMatch(
                text=match.group(),
                type=entity_type,
                normalized=match.group().upper().replace(" ", "")
            ))

    # Extract keywords - preserve important terms
    stop_words = {"the", "a", "an", "is", "are", "was", "were", "what", "how", "many",
                  "much", "in", "on", "at", "to", "for", "of", "and", "or", "with", "about",
                  "all", "any", "can", "could", "do", "does", "did", "have", "has", "had",
                  "this", "that", "these", "those", "there", "where", "when", "who", "which"}
    # Keep important terms even if short
    important_short_terms = {"fy", "q1", "q2", "q3", "q4", "us", "uk"}
    words = re.findall(r'\b[a-z0-9]+\b', query_lower)
    keywords = [w for w in words if (w not in stop_words and len(w) > 2) or w in important_short_terms]

    # Add compound keywords for common phrases
    if "quarterly" in query_lower:
        keywords.append("quarterly")
    if "annual" in query_lower:
        keywords.append("annual")
    if "forms" in query_lower or "form" in query_lower:
        keywords.append("forms")
    if "received" in query_lower:
        keywords.append("received")
    if "approved" in query_lower or "approval" in query_lower:
        keywords.append("approval")
    if "denied" in query_lower or "denial" in query_lower:
        keywords.append("denial")

    # Classify intent
    intent = "factual"
    intent_keywords = {
        "trend": ["trend", "over time", "change", "growth", "decline"],
        "comparison": ["compare", "versus", "vs", "difference", "between"],
        "definition": ["what is", "what are", "define", "meaning", "explain"],
    }
    for intent_type, kws in intent_keywords.items():
        if any(kw in query_lower for kw in kws):
            intent = intent_type
            break

    return QueryDecomposition(
        original_query=query,
        entities=entities,
        keywords=keywords[:10],
        intent=intent
    )


def _expand_query_with_graph(decomposition: QueryDecomposition) -> dict[str, Any]:
    """Use Neo4j to expand query with related entities and document suggestions.

    Returns dict with expanded_keywords and suggested_doc_ids.
    Fails silently if Neo4j is unavailable.
    """
    expanded = {"expanded_keywords": [], "suggested_doc_ids": []}

    graph_enabled = os.getenv("GRAPH_ENABLED", "false").lower() in ("true", "1", "yes")
    if not graph_enabled:
        return expanded

    try:
        from src.services import neo4j_client

        for entity in decomposition.entities:
            related = neo4j_client.find_related_entities(entity.normalized, depth=2, limit=5)
            for r in related:
                if r.get("name"):
                    expanded["expanded_keywords"].append(r["name"])

            docs = neo4j_client.find_documents_by_entity(entity.normalized, limit=5)
            for d in docs:
                if d.get("doc_id"):
                    expanded["suggested_doc_ids"].append(d["doc_id"])

    except Exception as e:
        logger.debug(f"[search] Graph expansion skipped: {e}")

    return expanded


def _enrich_results_with_graph(results: list[DocumentReference]) -> None:
    """Add graph context to search results (in-place). Fails silently."""
    graph_enabled = os.getenv("GRAPH_ENABLED", "false").lower() in ("true", "1", "yes")
    if not graph_enabled:
        return

    try:
        from src.services import neo4j_client

        for ref in results:
            try:
                context = neo4j_client.find_document_context(ref.document_id)
                ref.related_entities = [e["name"] for e in context.get("entities", [])[:10]]
                ref.related_documents = [d["asset"] for d in context.get("related_documents", [])[:5]]
                periods = context.get("time_periods", [])
                if periods:
                    ref.graph_context = f"Covers: {', '.join(p['period'] for p in periods)}"
            except Exception:
                pass
    except Exception as e:
        logger.debug(f"[search] Graph enrichment skipped: {e}")


def _search_with_weaviate(
    decomposition: QueryDecomposition,
    graph_expansion: dict[str, Any],
    storage: Storage | None = None,
    db: DBSession | None = None,
) -> tuple[list[DocumentReference], dict]:
    """Search using Weaviate hybrid search (BM25 + vector)."""
    from src.services import weaviate_client

    data_metrics = {
        "documents_searched": 0,
        "documents_returned": 0,
        "data_volume_bytes": 0,
        "semantic_search_used": True,
    }

    # Build query text from original query + expanded keywords
    query_parts = [decomposition.original_query]
    query_parts.extend(decomposition.keywords)
    query_parts.extend(graph_expansion.get("expanded_keywords", []))
    query_text = " ".join(query_parts)

    # Generate query embedding
    query_vector = get_embedding(decomposition.original_query)
    query_vector_list = query_vector.tolist() if query_vector is not None else None

    # Build filters from graph-suggested doc_ids
    filters = None
    suggested_ids = graph_expansion.get("suggested_doc_ids", [])
    if suggested_ids:
        filters = {"doc_ids": suggested_ids}

    # Search chunks first (granular), then fall back to documents
    chunk_results = weaviate_client.hybrid_search(
        query_text=query_text,
        query_vector=query_vector_list,
        filters=filters,
        limit=15,
        alpha=0.5,
        collection_name=weaviate_client.GOV_CHUNK_COLLECTION,
    )

    # If no chunk results, try document-level
    if not chunk_results:
        chunk_results = weaviate_client.hybrid_search(
            query_text=query_text,
            query_vector=query_vector_list,
            limit=15,
            alpha=0.5,
            collection_name=weaviate_client.GOV_DOCUMENT_COLLECTION,
        )

    data_metrics["documents_searched"] = len(chunk_results)

    # Apply cross-encoder reranker before deduplication
    reranker = get_reranker()
    if reranker.is_available and chunk_results:
        try:
            reranked = rerank_documents(
                decomposition.original_query, chunk_results, top_k=len(chunk_results)
            )
            # Blend reranker scores with original Weaviate scores
            for doc, rerank_score in reranked:
                original_score = doc.get("_score", 0.5)
                # RRF-style blending: 60% reranker + 40% original
                doc["_score"] = 0.6 * float(rerank_score) + 0.4 * float(original_score)

            # Re-sort by blended score
            chunk_results.sort(key=lambda x: x.get("_score", 0), reverse=True)
        except Exception as e:
            logger.warning(f"Reranker failed, using original scores: {e}")

    # Deduplicate by doc_id, keep best score per document
    seen_docs: dict[str, dict] = {}
    for result in chunk_results:
        doc_id = result.get("doc_id", "")
        score = result.get("_score", 0)
        if doc_id not in seen_docs or score > seen_docs[doc_id].get("_score", 0):
            seen_docs[doc_id] = result

    # Sort deduped results by score and take top 5
    sorted_docs = sorted(seen_docs.values(), key=lambda x: x.get("_score", 0), reverse=True)[:5]

    # Load full enriched documents from MinIO for rich metadata and context
    results: list[DocumentReference] = []
    full_docs: list[dict] = []

    # Build asset/agency lookup from DB if available
    asset_lookup: dict[str, Any] = {}
    agency_lookup: dict[int, Any] = {}
    if db:
        try:
            all_assets = db.query(AssetModel).all()
            asset_lookup = {a.name: a for a in all_assets}
            all_agencies = db.query(AgencyModel).all()
            agency_lookup = {a.id: a for a in all_agencies}
        except Exception:
            pass

    for result in sorted_docs:
        doc_id = result.get("doc_id", "")
        asset_name = result.get("asset", "")
        agency_name = result.get("agency", "")
        score = result.get("_score", 0)

        # Try to load the full enriched document from MinIO
        full_doc = None
        if storage and asset_name and agency_name:
            try:
                prefix = f"enrichment-zone/{agency_name}/{asset_name}/"
                objects = list(storage.client.list_objects(
                    storage.bucket, prefix=prefix, recursive=True
                ))
                json_objects = [
                    o for o in objects
                    if o.object_name.endswith(".json")
                    and not o.object_name.endswith("_metadata.json")
                ]
                if json_objects:
                    # Get latest
                    json_objects.sort(key=lambda o: o.last_modified, reverse=True)
                    data = storage.get_object(json_objects[0].object_name)
                    full_doc = json.loads(data.decode("utf-8"))
                    full_docs.append(full_doc)
            except Exception:
                pass

        if full_doc:
            # Use the rich _build_document_reference with full metadata
            ref = _build_document_reference(
                full_doc,
                doc_id or f"enrichment-zone/{agency_name}/{asset_name}/latest",
                decomposition,
                asset_lookup,
                agency_lookup,
                score,
            )
        else:
            # Fallback: bare reference from chunk properties
            text = result.get("text", result.get("summary", ""))
            snippet = text[:300] + "..." if len(text) > 300 else text
            ref = DocumentReference(
                document_id=doc_id,
                document_title=result.get("title", ""),
                asset_name=asset_name,
                agency_name=agency_name,
                agency_id=agency_name,
                section=result.get("section_id") or result.get("level"),
                relevance_score=round(score, 3),
                snippet=snippet,
            )

        results.append(ref)

    data_metrics["documents_returned"] = len(results)
    data_metrics["data_volume_bytes"] = sum(len(r.snippet.encode()) for r in results)
    data_metrics["full_docs"] = full_docs

    return results, data_metrics


# ---------------------------------------------------------------------------
# Source trust weights
# ---------------------------------------------------------------------------
# Trust tiers: local V+G data is highest, USAFacts is curated/high,
# .gov web is authoritative/moderate. All sources participate — weights
# adjust ranking, they don't exclude sources.

_SOURCE_TRUST = {
    "local":    1.00,   # Ingested + enriched (Weaviate + Neo4j)
    "usafacts": 0.85,   # USAFacts.org — curated, high quality
    "gov":      0.70,   # .gov web search — authoritative but broad
    "web":      0.60,   # Other web results
}

# Recency keywords that suggest the user wants current information
_RECENCY_KEYWORDS = [
    "current", "latest", "recent", "now", "today", "2025", "2026",
    "who is", "what is the current", "right now", "as of",
    "president", "director", "secretary", "acting",
    "new", "updated", "this year", "this quarter",
]


def _classify_source(doc_ref: "DocumentReference") -> str:
    """Classify a DocumentReference into a trust tier."""
    if doc_ref.document_id.startswith("web/"):
        url = (doc_ref.source_url or "").lower()
        if "usafacts.org" in url:
            return "usafacts"
        elif ".gov" in url:
            return "gov"
        return "web"
    return "local"


def _apply_trust_weights(
    results: list["DocumentReference"],
    is_recency_sensitive: bool,
) -> None:
    """Apply trust-based weighting to all results in-place.

    Trust hierarchy (non-recency queries):
        local (1.0) > usafacts (0.85) > .gov (0.70) > other web (0.60)

    For recency-sensitive queries, web sources get a recency bonus
    but local sources still compete — they aren't penalised, web is
    boosted to be *comparable* so both can surface.

    The final score is:  original_relevance × trust_weight [+ recency_bonus]
    """
    for ref in results:
        tier = _classify_source(ref)
        trust = _SOURCE_TRUST.get(tier, 0.60)

        # Base weighted score
        weighted = ref.relevance_score * trust

        # Recency bonus for web sources on time-sensitive queries
        if is_recency_sensitive and tier != "local":
            # Boost web sources so they can compete with local data
            # USAFacts gets a smaller boost (already high trust)
            recency_bonus = 0.15 if tier == "usafacts" else 0.25
            weighted = min(weighted + recency_bonus, 0.99)

        ref.relevance_score = round(weighted, 3)


def _search_web(query: str, decomposition: "QueryDecomposition") -> list[DocumentReference]:
    """Search .gov and usafacts.org via Firecrawl.

    Returns DocumentReferences with web content in the snippet field
    and the source URL for proper attribution. Each result carries its
    raw search-rank relevance; trust weighting is applied later by
    _apply_trust_weights() so all sources are weighted consistently.
    """
    from src.services.web_search import search_gov_sources

    try:
        web_results = search_gov_sources(query, limit=5)
    except Exception as e:
        logger.warning(f"[search] Web search failed: {e}")
        return []

    refs = []
    for i, result in enumerate(web_results):
        content = result.get("content", "")
        url = result.get("url", "")
        title = result.get("title", "Web Result")
        domain = result.get("source_domain", "web")

        snippet = content[:1500] + "..." if len(content) > 1500 else content

        # Readable agency name from URL
        agency_display = domain
        if "uscis.gov" in url:
            agency_display = "USCIS (web)"
        elif "dhs.gov" in url:
            agency_display = "DHS (web)"
        elif "usafacts.org" in url:
            agency_display = "USAFacts (web)"
        elif ".gov" in url:
            from urllib.parse import urlparse
            host = urlparse(url).hostname or ""
            parts = host.replace(".gov", "").split(".")
            agency_display = f"{parts[-1].upper()} (web)" if parts else "GOV (web)"

        # File format from URL
        file_format = "WEB"
        url_lower = url.lower()
        if url_lower.endswith(".xlsx") or url_lower.endswith(".xls"):
            file_format = "XLSX"
        elif url_lower.endswith(".pdf"):
            file_format = "PDF"
        elif url_lower.endswith(".csv"):
            file_format = "CSV"

        clean_title = re.sub(r'^\[(?:XLS|PDF|CSV|DOC)\]\s*', '', title).strip()

        # Raw relevance from search rank (will be reweighted by trust later)
        raw_relevance = round(0.80 - (i * 0.04), 3)

        ref = DocumentReference(
            document_id=f"web/{domain}/{i}",
            document_title=clean_title or title,
            asset_name=f"web-search",
            agency_name=agency_display,
            agency_id="web",
            section=None,
            relevance_score=raw_relevance,
            snippet=snippet,
            original_url=None,
            source_url=url,
            file_format=file_format,
        )
        refs.append(ref)

    return refs


def _deduplicate_web_results(
    local_results: list[DocumentReference],
    web_results: list[DocumentReference],
) -> list[DocumentReference]:
    """Remove web results that duplicate already-ingested local documents.

    Matches by:
    1. Exact source URL match (the same file URL)
    2. Filename match (e.g., both reference "net_backlog_frontlog_fy2025_q3.xlsx")
    3. Asset name overlap (web title contains the local asset name or vice versa)
    """
    if not local_results or not web_results:
        return web_results

    # Build lookup sets from local results
    local_urls: set[str] = set()
    local_filenames: set[str] = set()
    local_assets: set[str] = set()

    for lr in local_results:
        # Collect source URLs
        for url in [lr.source_url, lr.original_url]:
            if url:
                local_urls.add(url.lower().rstrip("/"))
                # Extract filename from URL
                filename = url.rsplit("/", 1)[-1].split("?")[0].lower()
                if "." in filename:
                    local_filenames.add(filename)

        # Collect asset names
        if lr.asset_name:
            local_assets.add(lr.asset_name.lower())

    # Filter web results
    deduped = []
    for wr in web_results:
        web_url = (wr.source_url or "").lower().rstrip("/")
        is_duplicate = False

        # Check 1: Exact URL match
        if web_url and web_url in local_urls:
            is_duplicate = True

        # Check 2: Same filename (e.g., both point to the same .xlsx)
        if not is_duplicate and web_url:
            web_filename = web_url.rsplit("/", 1)[-1].split("?")[0]
            if web_filename and "." in web_filename and web_filename in local_filenames:
                is_duplicate = True

        # Check 3: Web title closely matches a local asset name
        if not is_duplicate:
            web_title_lower = (wr.document_title or "").lower()
            for asset in local_assets:
                # Normalize: "uscis-backlog-frontlog" → "backlog frontlog"
                asset_words = asset.replace("-", " ").replace("_", " ")
                # Check if the core words overlap significantly
                asset_tokens = set(asset_words.split())
                title_tokens = set(web_title_lower.replace("-", " ").replace("_", " ").split())
                if len(asset_tokens) >= 2 and len(asset_tokens & title_tokens) >= len(asset_tokens) * 0.6:
                    is_duplicate = True
                    break

        if is_duplicate:
            logger.debug(f"[search] Dropping duplicate web result: {wr.document_title}")
        else:
            deduped.append(wr)

    return deduped


def search_documents(
    storage: Storage,
    db: DBSession,
    decomposition: QueryDecomposition,
    mode: str | None = None,
) -> tuple[list[DocumentReference], dict]:
    """Search with configurable mode.

    Modes:
        v   - Weaviate only
        vg  - Weaviate + Neo4j graph
        vw  - Weaviate + web search
        vgw - Weaviate + Neo4j + web search (default if GRAPH_ENABLED)

    Also supports legacy mode names:
        weaviate_only  -> v
        weaviate_graph -> vg

    Returns tuple of (documents, data_metrics_dict)
    """
    # Normalize mode names (support both old and new)
    mode_map = {
        "weaviate_only": "v",
        "weaviate_graph": "vg",
        "v": "v", "vg": "vg", "vw": "vw", "vgw": "vgw",
    }
    effective_mode = mode_map.get(mode or "", "vg" if os.getenv("GRAPH_ENABLED", "").lower() in ("true", "1") else "v")

    use_graph = effective_mode in ("vg", "vgw")
    use_web = effective_mode in ("vw", "vgw")

    graph_expansion = _expand_query_with_graph(decomposition) if use_graph else {"expanded_keywords": [], "suggested_doc_ids": []}

    try:
        results, data_metrics = _search_with_weaviate(decomposition, graph_expansion, storage, db)

        if use_graph:
            _enrich_results_with_graph(results)

        if use_web:
            web_results = _search_web(decomposition.original_query, decomposition)
            if web_results:
                # Deduplicate: remove web results that point to files we already
                # have as local ingested documents (same URL or same filename).
                web_results = _deduplicate_web_results(results, web_results)
                results.extend(web_results)
                data_metrics["web_results_added"] = len(web_results)

        # Apply trust-based weighting across ALL sources (local + web)
        query_lower = decomposition.original_query.lower()
        is_recency_sensitive = any(kw in query_lower for kw in _RECENCY_KEYWORDS)
        _apply_trust_weights(results, is_recency_sensitive)

        # Sort by weighted score and keep top results
        results.sort(key=lambda r: r.relevance_score, reverse=True)
        results = results[:7]

        return results, data_metrics
    except Exception as e:
        logger.error(f"[search] Search failed: {e}")
        return [], {
            "documents_searched": 0,
            "documents_returned": 0,
            "data_volume_bytes": 0,
            "semantic_search_used": False,
        }




def _build_document_reference(
    doc: dict,
    file_path: str,
    decomposition: QueryDecomposition,
    asset_lookup: dict,
    agency_lookup: dict,
    score: float,
) -> DocumentReference:
    """Build DocumentReference from document data."""
    enrichment = doc.get("enrichment", {})
    document_info = enrichment.get("document", {})
    metadata = doc.get("metadata", {})
    source = doc.get("source", {})

    # Extract asset name from file path
    parts = file_path.split("/")
    asset_name = parts[2] if len(parts) >= 4 else ""

    # Get proper document title from metadata
    doc_title = metadata.get("title", "")
    if not doc_title or doc_title == asset_name:
        doc_title = asset_name.replace("-", " ").replace("_", " ").title()
        doc_title = doc_title.replace("Uscis", "USCIS")
        doc_title = doc_title.replace("Fy", "FY")
        doc_title = doc_title.replace("Daca", "DACA")
        doc_title = doc_title.replace("Tps", "TPS")

    # Get full agency name from database
    asset = asset_lookup.get(asset_name)
    agency_id = str(asset.agency_id) if asset else ""
    agency = agency_lookup.get(asset.agency_id) if asset else None
    agency_name = agency.name if agency else source.get("agency", metadata.get("agency", "Unknown Agency"))

    # Get source URLs and file info
    original_url = source.get("storageUrl", "")
    source_url = source.get("originalUrl", "")
    page_count = source.get("pageCount")
    file_format = metadata.get("format", source.get("mimeType", "")).split("/")[-1].upper()
    if not file_format:
        filename = source.get("filename", "")
        if filename:
            file_format = filename.split(".")[-1].upper() if "." in filename else None

    # Get sections
    content = doc.get("content", {})
    sections = content.get("sections", [])
    enrichment_sections = enrichment.get("sections", [])

    best_section = find_best_section(sections, decomposition)

    page_number = None
    sheet_name = None
    section_title = None
    section_summary = None

    if best_section:
        page_number = best_section.get("pageNumber")
        sheet_name = best_section.get("sheetName")
        section_title = best_section.get("title")

        if enrichment_sections:
            for es in enrichment_sections:
                if es.get("title") == section_title or es.get("originalIndex") == sections.index(best_section):
                    section_summary = es.get("summary")
                    break

    # Build snippet
    snippet = ""
    if best_section:
        snippet = best_section.get("content", "")[:400]
    if not snippet:
        summary = document_info.get("summary", "")
        snippet = summary[:400] if summary else ""
    if not snippet and sections:
        snippet = sections[0].get("content", "")[:400]

    return DocumentReference(
        document_id=file_path,
        document_title=doc_title,
        asset_name=asset_name,
        agency_name=agency_name,
        agency_id=agency_id,
        section=section_title,
        section_summary=section_summary,
        relevance_score=round(score, 3),
        snippet=snippet + "..." if snippet else "",
        original_url=original_url if original_url else None,
        source_url=source_url if source_url else None,
        page_count=page_count,
        page_number=page_number,
        sheet_name=sheet_name,
        file_format=file_format if file_format else None,
    )


def calculate_relevance_score(doc: dict[str, Any], decomposition: QueryDecomposition) -> float:
    """Calculate relevance score for a document with improved matching."""
    score = 0.0
    matches = 0
    total_checks = 0

    enrichment = doc.get("enrichment", {})
    document_info = enrichment.get("document", {})
    metadata = doc.get("metadata", {})
    content = doc.get("content", {})

    # Build searchable text corpus from document
    summary = document_info.get("summary", "").lower()
    title = metadata.get("title", "").lower()
    asset_name = doc.get("source", {}).get("asset", "").lower()
    if not asset_name:
        asset_name = metadata.get("asset_name", "").lower()

    # Get section content for deeper search
    sections = content.get("sections", [])
    section_text = " ".join([s.get("content", "") for s in sections[:10]]).lower()  # First 10 sections

    # Build full text corpus
    full_text = f"{title} {asset_name} {summary} {section_text}"

    doc_entities = document_info.get("entities", [])
    doc_topics = document_info.get("keyTopics", [])
    doc_entity_texts = [e.get("text", "").lower() for e in doc_entities]
    doc_topic_texts = [t.lower() if isinstance(t, str) else t.get("name", "").lower() for t in doc_topics]

    # Normalize fiscal year patterns for matching
    def normalize_fiscal_year(text: str) -> list[str]:
        """Generate variations of fiscal year/quarter patterns."""
        import re
        variations = [text.lower()]
        # FY2025 Q3 -> fy2025, q3, 2025, fiscal year 2025, quarter 3
        fy_match = re.match(r'fy\s*(\d{2,4})\s*q?(\d)?', text.lower())
        if fy_match:
            year = fy_match.group(1)
            if len(year) == 2:
                year = "20" + year
            variations.extend([
                f"fy{year}", f"fy {year}", f"fiscal year {year}",
                year, f"20{year[-2:]}" if len(year) == 4 else year
            ])
            if fy_match.group(2):
                q = fy_match.group(2)
                variations.extend([
                    f"q{q}", f"quarter {q}", f"q{q} {year}", f"{year} q{q}"
                ])
        return variations

    # Score entities with fuzzy matching
    for entity in decomposition.entities:
        total_checks += 1
        entity_lower = entity.text.lower()
        entity_variations = normalize_fiscal_year(entity.text) if entity.type in ("fiscal_year", "date") else [entity_lower]

        matched = False
        for variation in entity_variations:
            if variation in title or variation in asset_name:
                score += 0.5
                matched = True
                break
            elif variation in summary:
                score += 0.3
                matched = True
                break
            elif variation in full_text:
                score += 0.2
                matched = True
                break

        if not matched:
            # Check enriched entities
            if any(entity_lower in e or any(v in e for v in entity_variations) for e in doc_entity_texts):
                score += 0.25
                matched = True

        if matched:
            matches += 1

    # Score keywords with content search
    for keyword in decomposition.keywords:
        total_checks += 1
        keyword_lower = keyword.lower()

        matched = False
        if keyword_lower in title or keyword_lower in asset_name:
            score += 0.3
            matched = True
        elif keyword_lower in summary:
            score += 0.2
            matched = True
        elif keyword_lower in full_text:
            score += 0.15
            matched = True

        if not matched:
            # Check topics
            if any(keyword_lower in t for t in doc_topic_texts):
                score += 0.15
                matched = True

        if matched:
            matches += 1

    # Bonus for matching document type/category
    query_lower = decomposition.original_query.lower()
    if "quarterly" in query_lower and "quarterly" in full_text:
        score += 0.2
    if "annual" in query_lower and ("annual" in full_text or "yearly" in full_text):
        score += 0.2
    if "forms" in query_lower and "forms" in full_text:
        score += 0.15

    # Calculate final score based on match ratio
    if total_checks > 0:
        match_ratio = matches / total_checks
        # Combine raw score with match ratio
        # High match ratio boosts score, low ratio dampens it
        score = score * (0.5 + 0.5 * match_ratio)

    # Normalize to 0-1 range
    # Typical max score: ~2.0 with good matches
    score = min(score / 1.5, 1.0)

    return score


def find_best_section(sections: list[dict], decomposition: QueryDecomposition) -> dict | None:
    """Find the most relevant section."""
    if not sections:
        return None

    best_section = None
    best_score = 0

    for section in sections:
        section_title = section.get("title", "").lower()
        section_content = section.get("content", "").lower()

        score = 0
        for entity in decomposition.entities:
            if entity.text.lower() in section_title:
                score += 0.5
            if entity.text.lower() in section_content:
                score += 0.2

        for keyword in decomposition.keywords:
            if keyword in section_title:
                score += 0.3
            if keyword in section_content:
                score += 0.1

        if score > best_score:
            best_score = score
            best_section = section

    return best_section


def find_relevant_sections(
    sections: list[dict],
    decomposition: QueryDecomposition,
    max_sections: int = 3
) -> list[dict]:
    """Find the most relevant sections for the query.

    Returns sections sorted by relevance with their scores.
    """
    if not sections:
        return []

    scored_sections = []

    for section in sections:
        section_title = section.get("title", "").lower()
        section_content = section.get("content", "").lower()

        score = 0
        for entity in decomposition.entities:
            entity_lower = entity.text.lower()
            if entity_lower in section_title:
                score += 0.5
            if entity_lower in section_content:
                score += 0.3

        for keyword in decomposition.keywords:
            kw_lower = keyword.lower()
            if kw_lower in section_title:
                score += 0.3
            if kw_lower in section_content:
                score += 0.15

        if score > 0:
            scored_sections.append((score, section))

    # Sort by score and return top sections
    scored_sections.sort(key=lambda x: x[0], reverse=True)
    return [section for _, section in scored_sections[:max_sections]]


def find_relevant_tables(
    tables: list[dict],
    decomposition: QueryDecomposition,
    max_tables: int = 2
) -> list[dict]:
    """Find the most relevant tables for the query.

    Checks table titles, headers, and content for matches.
    """
    if not tables:
        return []

    scored_tables = []

    for table in tables:
        table_title = table.get("title", "").lower()
        headers = [str(h).lower() for h in table.get("headers", [])]
        rows = table.get("rows", [])

        # Build searchable content from first few rows
        row_content = ""
        for row in rows[:5]:
            row_content += " ".join(str(cell).lower() for cell in row if cell) + " "

        score = 0

        # Score based on query matches
        for entity in decomposition.entities:
            entity_lower = entity.text.lower()
            if entity_lower in table_title:
                score += 0.5
            if any(entity_lower in h for h in headers):
                score += 0.4
            if entity_lower in row_content:
                score += 0.3

        for keyword in decomposition.keywords:
            kw_lower = keyword.lower()
            if kw_lower in table_title:
                score += 0.3
            if any(kw_lower in h for h in headers):
                score += 0.25
            if kw_lower in row_content:
                score += 0.15

        if score > 0:
            scored_tables.append((score, table))

    # Sort by score and return top tables
    scored_tables.sort(key=lambda x: x[0], reverse=True)
    return [table for _, table in scored_tables[:max_tables]]


def generate_answer_with_claude(
    query: str,
    decomposition: QueryDecomposition,
    documents: list[DocumentReference],
    full_docs: list[dict] | None = None
) -> tuple[str, float, dict, ChartSpec | None]:
    """Generate an answer using Claude with rich context from retrieved documents.

    Passes full sections and tables (with page/sheet references) to Claude
    for more accurate and detailed answers.

    Returns tuple of (answer, confidence, token_usage_dict, chart_specs_list, context_text)
    """
    token_usage = {"input_tokens": 0, "output_tokens": 0}

    if not documents:
        return (
            "I couldn't find any relevant documents to answer your question. "
            "Try asking about USCIS forms (I-130, I-140, etc.), visa programs (H-1B, H-2B), "
            "or immigration programs (DACA, TPS).",
            0.0,
            token_usage,
            [],
            "",
        )

    client = get_anthropic_client()
    if not client:
        answer, confidence = generate_answer_fallback(query, decomposition, documents)
        return answer, confidence, token_usage, [], ""

    # Build rich context from full documents
    context_parts = []
    full_docs = full_docs or []

    # Track which full_docs index to use (skip web results)
    full_doc_idx = 0

    for i, doc_ref in enumerate(documents[:5], 1):
        is_web_source = doc_ref.document_id.startswith("web/")

        context_part = f"\n## Source {i}: {doc_ref.document_title}\n"
        context_part += f"**Source type:** {'Web search result' if is_web_source else 'Ingested document'}\n"
        context_part += f"**Agency:** {doc_ref.agency_name}\n"

        if is_web_source:
            # Web source — include URL and full content from snippet
            if doc_ref.source_url:
                context_part += f"**URL:** {doc_ref.source_url}\n"
            context_part += f"**Relevance:** {doc_ref.relevance_score * 100:.0f}%\n"
            if doc_ref.snippet:
                context_part += f"\n**Content:**\n{doc_ref.snippet}\n"
            context_parts.append(context_part)
            continue

        # Local document — use full enriched content from MinIO
        full_doc = full_docs[full_doc_idx] if full_doc_idx < len(full_docs) else None
        full_doc_idx += 1

        context_part += f"**Asset:** {doc_ref.asset_name}\n"
        context_part += f"**Format:** {doc_ref.file_format or 'Unknown'}\n"
        context_part += f"**Relevance:** {doc_ref.relevance_score * 100:.0f}%\n"

        # Add location information if available
        if doc_ref.page_number:
            context_part += f"**Location:** Page {doc_ref.page_number}"
            if doc_ref.page_count:
                context_part += f" of {doc_ref.page_count}"
            context_part += "\n"
        elif doc_ref.sheet_name:
            context_part += f"**Location:** Sheet '{doc_ref.sheet_name}'\n"

        if full_doc:
            # Add enrichment summary
            enrichment = full_doc.get("enrichment", {})
            doc_enrichment = enrichment.get("document", {})
            summary = doc_enrichment.get("summary", "")
            if summary:
                context_part += f"\n**Summary:** {summary}\n"

            # Add full relevant sections with page references
            content = full_doc.get("content", {})
            sections = content.get("sections", [])

            # Find relevant sections based on query keywords
            relevant_sections = find_relevant_sections(sections, decomposition, max_sections=3)
            if relevant_sections:
                context_part += "\n**Relevant Sections:**\n"
                for section in relevant_sections:
                    section_title = section.get("title", "Untitled Section")
                    section_content = section.get("content", "")
                    page_num = section.get("pageNumber")
                    sheet = section.get("sheetName")

                    location_info = ""
                    if page_num:
                        location_info = f" (Page {page_num})"
                    elif sheet:
                        location_info = f" (Sheet: {sheet})"

                    context_part += f"\n### {section_title}{location_info}\n"
                    # Truncate long sections but keep more than just snippet
                    context_part += section_content[:1500] + ("..." if len(section_content) > 1500 else "")
                    context_part += "\n"

            # Add full relevant tables with data
            tables = content.get("tables", [])
            relevant_tables = find_relevant_tables(tables, decomposition, max_tables=2)
            if relevant_tables:
                context_part += "\n**Relevant Tables:**\n"
                for table in relevant_tables:
                    table_title = table.get("title", "Data Table")
                    headers = table.get("headers", [])
                    rows = table.get("rows", [])
                    page_num = table.get("pageNumber")
                    sheet = table.get("sheetName")

                    location_info = ""
                    if page_num:
                        location_info = f" (Page {page_num})"
                    elif sheet:
                        location_info = f" (Sheet: {sheet})"

                    context_part += f"\n### {table_title}{location_info}\n"
                    if headers:
                        context_part += "| " + " | ".join(str(h) for h in headers) + " |\n"
                        context_part += "| " + " | ".join("---" for _ in headers) + " |\n"
                    # Include more rows for factual queries
                    row_limit = 15 if decomposition.intent == "factual" else 8
                    for row in rows[:row_limit]:
                        context_part += "| " + " | ".join(str(cell) if cell else "" for cell in row) + " |\n"
                    if len(rows) > row_limit:
                        context_part += f"\n*... {len(rows) - row_limit} more rows*\n"
        else:
            # Fallback to snippet if full doc not available
            if doc_ref.section:
                context_part += f"\n**Section:** {doc_ref.section}\n"
            if doc_ref.section_summary:
                context_part += f"{doc_ref.section_summary}\n"
            context_part += f"\n**Content excerpt:** {doc_ref.snippet}\n"

        context_parts.append(context_part)

    context = "\n---\n".join(context_parts)

    try:
        message = client.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=1500,
            messages=[
                {
                    "role": "user",
                    "content": f"""You are a Q&A assistant helping users understand government data. Today's date is {datetime.now().strftime('%B %d, %Y')}.

Answer the user's question based on the provided sources. Sources may include ingested government documents AND live web search results from .gov and usafacts.org.

USER QUESTION: {query}

QUERY ANALYSIS:
- Identified entities: {', '.join(e.text for e in decomposition.entities) or 'None'}
- Query intent: {decomposition.intent}

RETRIEVED SOURCES:
{context}

INSTRUCTIONS:
1. Answer the question based on the sources provided. Prefer the most RECENT information when sources conflict.
2. Web search results are typically more current than ingested documents. If the question is about current facts (who holds a position, current policies, latest numbers), prioritize web sources.
3. CRITICAL — EVERY factual sentence MUST include an inline source citation. Use these formats:
   - For documents: (Source: **[Title]** from **[Agency]**)
   - For web results: (Source: [Page Title], [URL])
   - For bullet points listing data from the same source, cite the source at the start of the list, then each bullet inherits that citation.
   Do NOT write any factual claim without a parenthetical source reference. This is the most important rule.
4. Keep the answer concise but informative (2-4 paragraphs max)
5. Use markdown formatting for readability (bold for emphasis, bullet points if listing items)
6. When referencing numbers, include the time period and source
8. If the data contains numerical information, assess which chart types would meaningfully help answer the question. Include MULTIPLE chart specifications at the END of your response, each in its own block. Consider:
   - "bar": for comparing categories or ranking items
   - "line": for showing trends over time or sequential data
   - "pie": for showing proportions/distributions of a whole
   Only include charts that ADD VALUE to the answer. Each chart should show a DIFFERENT perspective on the data. Use this EXACT format for EACH chart:
```chart
{{"chart_type": "bar", "title": "Chart Title", "x_label": "X Axis", "y_label": "Y Axis", "data": [{{"label": "A", "value": 123}}, {{"label": "B", "value": 456}}]}}
```
You may include 1 to 3 chart blocks. Do NOT force charts when data doesn't support them.

Provide your answer:"""
                }
            ]
        )

        # Capture token usage
        token_usage["input_tokens"] = message.usage.input_tokens
        token_usage["output_tokens"] = message.usage.output_tokens

        answer_text = message.content[0].text.strip()
        confidence = documents[0].relevance_score if documents else 0.0

        # Parse all chart specifications from the answer
        chart_specs: list[ChartSpec] = []
        chart_matches = list(re.finditer(r'```chart\s*(.*?)\s*```', answer_text, re.DOTALL))
        if chart_matches:
            # Strip all chart blocks from the answer text
            clean_answer = answer_text
            for match in reversed(chart_matches):
                clean_answer = clean_answer[:match.start()] + clean_answer[match.end():]
            answer_text = clean_answer.strip()

            for match in chart_matches:
                try:
                    chart_data = json.loads(match.group(1))
                    chart_specs.append(ChartSpec(**chart_data))
                except Exception:
                    pass

        return answer_text, confidence, token_usage, chart_specs, context

    except Exception as e:
        logger.error(f"Error in generate_answer_with_claude: {e}")
        # Fallback if Claude fails
        answer, confidence = generate_answer_fallback(query, decomposition, documents)
        return answer, confidence, token_usage, [], ""


def generate_answer_fallback(
    query: str,
    decomposition: QueryDecomposition,
    documents: list[DocumentReference]
) -> tuple[str, float]:
    """Fallback answer generation without LLM."""
    if not documents:
        return ("No relevant documents found.", 0.0)

    top_doc = documents[0]
    confidence = top_doc.relevance_score

    entity_mentions = ""
    if decomposition.entities:
        entity_texts = [e.text for e in decomposition.entities]
        entity_mentions = f"Based on your query about **{', '.join(entity_texts)}**, "
    else:
        entity_mentions = "Based on your query, "

    answer_parts = [entity_mentions]
    answer_parts.append(
        f"I found relevant information in **{top_doc.document_title}** from **{top_doc.agency_name}**."
    )

    if top_doc.section:
        answer_parts.append(f"\n\nThe most relevant section is **\"{top_doc.section}\"**.")
        if top_doc.section_summary:
            answer_parts.append(f" {top_doc.section_summary}")

    if top_doc.snippet:
        answer_parts.append(f"\n\n> {top_doc.snippet}")

    if len(documents) > 1:
        other_docs = [f"**{d.document_title}**" for d in documents[1:3]]
        answer_parts.append(f"\n\nAdditional sources: {', '.join(other_docs)}.")

    return "".join(answer_parts), confidence


@router.post(
    "/query",
    response_model=SearchResult,
    summary="Search Documents with Natural Language",
    response_description="Search results with AI-generated answer and source documents",
)
def search_query(request: QueryRequest, storage: Storage, db: DBSession) -> SearchResult:
    """
    Process a natural language query and return relevant documents with AI-generated answers.

    This endpoint powers the Q&A assistant chatbot using a RAG (Retrieval-Augmented Generation)
    approach:

    1. **Query Decomposition**: Claude AI analyzes the query to extract entities (forms, programs,
       dates) and determine intent (factual, comparison, trend, definition)

    2. **Hybrid Search**: Weaviate-powered BM25 + vector search with Neo4j graph expansion

    3. **Answer Generation**: Claude generates a comprehensive answer citing source documents

    **Example queries:**
    - "What is the H-1B visa program?"
    - "How many I-130 forms were processed in FY2024?"
    - "Compare DACA and TPS programs"

    **Response includes:**
    - AI-generated answer with markdown formatting
    - Source documents with relevance scores
    - Query decomposition showing extracted entities
    - Usage metrics (tokens, data volume)
    """
    # Decompose the query using Claude
    decomposition, decompose_tokens = decompose_query_with_claude(request.query)

    # Search documents (includes full docs for rich context)
    documents, data_metrics = search_documents(storage, db, decomposition, mode=request.mode)

    # Extract full docs for rich context (if available)
    full_docs = data_metrics.pop("full_docs", [])

    # Generate answer using Claude with rich context
    answer, confidence, answer_tokens, chart_specs, claude_context = generate_answer_with_claude(
        request.query, decomposition, documents, full_docs
    )

    # Aggregate token usage
    total_input_tokens = decompose_tokens["input_tokens"] + answer_tokens["input_tokens"]
    total_output_tokens = decompose_tokens["output_tokens"] + answer_tokens["output_tokens"]
    total_tokens = total_input_tokens + total_output_tokens

    # Determine if Claude was successfully used (tokens > 0 means API was called)
    claude_used = total_tokens > 0

    # Calculate context window usage percentage
    context_window_percent = (total_tokens / CLAUDE_CONTEXT_WINDOW) * 100

    # Generate brief answer (first sentence or up to 150 chars)
    brief_answer = ""
    if documents:
        if claude_used:
            # Extract first sentence from Claude's answer
            first_sentence = answer.split(".")[0] if "." in answer else answer[:150]
            brief_answer = first_sentence.strip().replace("**", "") + "."
        else:
            # Generate brief from top document
            top_doc = documents[0]
            brief_answer = f"Found information about {', '.join(e.text for e in decomposition.entities) or 'your query'} in {top_doc.document_title}."
    else:
        brief_answer = "No relevant documents found for your query."

    # Build usage metrics
    usage = UsageMetrics(
        input_tokens=total_input_tokens,
        output_tokens=total_output_tokens,
        total_tokens=total_tokens,
        context_window_used_percent=round(context_window_percent, 2),
        documents_searched=data_metrics["documents_searched"],
        documents_returned=data_metrics["documents_returned"],
        data_volume_bytes=data_metrics["data_volume_bytes"],
        data_volume_display=format_data_volume(data_metrics["data_volume_bytes"]),
    )

    # Compute answer quality metrics using the full context Claude received
    from src.services.answer_metrics import compute_answer_metrics

    doc_dicts = [
        {
            "document_title": d.document_title,
            "agency_name": d.agency_name,
            "snippet": d.snippet,
            "source_url": d.source_url,
            "full_context": claude_context,  # The full text Claude saw
        }
        for d in documents
    ]
    raw_metrics = compute_answer_metrics(answer, doc_dicts, request.query)
    answer_metrics = AnswerMetrics(**raw_metrics)

    logger.info(f"Search completed: claude_used={claude_used}, tokens={total_tokens}, docs={len(documents)}")

    return SearchResult(
        query_decomposition=decomposition,
        documents=documents,
        answer=answer,
        brief_answer=brief_answer,
        confidence=confidence,
        charts=chart_specs,
        usage=usage,
        claude_used=claude_used,
        metrics=answer_metrics,
    )


@router.get(
    "/entities",
    summary="Get Available Entities for Autocomplete",
    response_description="Lists of forms, programs, organizations, and topics found in documents",
)
def get_available_entities(storage: Storage) -> dict[str, list[str]]:
    """
    Get all entities extracted from enriched documents, grouped by type.

    Used by the Q&A assistant UI for autocomplete suggestions and topic browsing.

    **Returns categories:**
    - `forms`: Immigration forms (I-130, I-140, N-400, etc.)
    - `visa_types`: Visa categories (H-1B, H-2B, L-1, etc.)
    - `programs`: Government programs (DACA, TPS, VAWA, etc.)
    - `organizations`: Agencies and organizations
    - `topics`: Key topics extracted from document content
    """
    entities: dict[str, set[str]] = {
        "forms": set(),
        "visa_types": set(),
        "programs": set(),
        "organizations": set(),
        "topics": set(),
    }

    try:
        enriched_files = storage.list_objects("enrichment-zone/")

        asset_latest: dict[str, str] = {}
        for file_path in enriched_files:
            if not file_path.endswith(".json"):
                continue
            parts = file_path.split("/")
            if len(parts) >= 4:
                asset_name = parts[2]
                asset_latest[asset_name] = file_path

        for file_path in asset_latest.values():
            doc = storage.get_json_object(file_path)
            if not doc:
                continue

            enrichment = doc.get("enrichment", {})
            doc_entities = enrichment.get("document", {}).get("entities", [])
            doc_topics = enrichment.get("document", {}).get("keyTopics", [])

            for entity in doc_entities:
                entity_type = entity.get("type", "").lower()
                entity_text = entity.get("text", "").strip()

                # Skip empty entity text
                if not entity_text:
                    continue

                if "form" in entity_type or re.match(r"I-\d{3}", entity_text):
                    entities["forms"].add(entity_text)
                elif "organization" in entity_type or "agency" in entity_type:
                    entities["organizations"].add(entity_text)
                elif "program" in entity_type:
                    entities["programs"].add(entity_text)

            for topic in doc_topics:
                topic_text = topic if isinstance(topic, str) else topic.get("name", "")
                if topic_text:
                    entities["topics"].add(topic_text)

    except Exception:
        pass

    return {k: sorted(list(v)) for k, v in entities.items()}


@router.get(
    "/status",
    summary="Debug: Check Search Service Status",
    response_description="Debug information about API key configuration and client status",
)
def get_search_status() -> dict[str, Any]:
    """
    Debug endpoint to check if the Anthropic API is properly configured.

    Returns information about:
    - Environment file location and existence
    - Whether ANTHROPIC_API_KEY is set
    - Whether the Anthropic client was initialized successfully

    **Note:** This is a debug endpoint for troubleshooting API configuration issues.
    """
    # Try loading .env again
    env_path = Path(__file__).parent.parent.parent.parent / ".env"

    # Read the file directly to check if it has the key
    env_content = None
    has_key_in_file = False
    if env_path.exists():
        env_content = env_path.read_text()
        has_key_in_file = "ANTHROPIC_API_KEY" in env_content

    api_key = os.getenv("ANTHROPIC_API_KEY")
    base_url = os.getenv("ANTHROPIC_BASE_URL")
    client = get_anthropic_client()

    # List all env vars that start with ANTHROPIC
    anthropic_vars = {k: v[:20] + "..." if v else None for k, v in os.environ.items() if "ANTHROPIC" in k.upper()}

    return {
        "env_path": str(env_path),
        "env_path_exists": env_path.exists(),
        "env_has_anthropic_key": has_key_in_file,
        "anthropic_api_key_set": bool(api_key),
        "anthropic_api_key_prefix": api_key[:20] + "..." if api_key else None,
        "anthropic_base_url": base_url,
        "anthropic_client_initialized": client is not None,
        "anthropic_env_vars": anthropic_vars,
        "all_env_keys_count": len(os.environ),
    }


@router.post(
    "/refresh-index",
    summary="Refresh Weaviate Search Index",
    response_description="Status of the index refresh operation with document and chunk counts",
)
def refresh_search_index(storage: Storage) -> dict[str, Any]:
    """
    Re-sync enriched documents and chunks from MinIO into Weaviate.

    Call this endpoint after running the ingestion pipeline to include newly
    processed documents in search results.

    **When to call:**
    - After running `pipeline run --workflow <name>`
    - After batch processing multiple workflows
    - When search results seem stale or missing new documents
    """
    try:
        from src.services import weaviate_client

        result = weaviate_client.sync_from_storage(storage)
        return {
            "status": "success",
            "message": "Weaviate index refreshed successfully",
            "documents_indexed": result["documents"],
            "chunks_indexed": result["chunks"],
        }
    except Exception as e:
        logger.error(f"[search] Error refreshing Weaviate index: {e}")
        return {
            "status": "error",
            "message": str(e),
            "documents_indexed": 0,
            "chunks_indexed": 0,
        }


@router.get(
    "/index-status",
    summary="Get Search Index Status",
    response_description="Current Weaviate index status including document and chunk counts",
)
def get_index_status() -> dict[str, Any]:
    """
    Get the current status of the Weaviate search index.

    **Response fields:**
    - `status`: "ready" | "not_initialized" | "error"
    - `documents_indexed`: Number of documents in the index
    - `chunks_indexed`: Number of chunks in the index
    - `message`: Human-readable status message
    """
    try:
        from src.services import weaviate_client

        client = weaviate_client.get_client()
        doc_collection = client.collections.get(weaviate_client.GOV_DOCUMENT_COLLECTION)
        chunk_collection = client.collections.get(weaviate_client.GOV_CHUNK_COLLECTION)

        doc_count = doc_collection.aggregate.over_all(total_count=True).total_count
        chunk_count = chunk_collection.aggregate.over_all(total_count=True).total_count

        return {
            "status": "ready",
            "documents_indexed": doc_count,
            "chunks_indexed": chunk_count,
            "message": f"Weaviate index ready with {doc_count} documents and {chunk_count} chunks",
        }
    except Exception as e:
        return {
            "status": "not_initialized",
            "documents_indexed": 0,
            "chunks_indexed": 0,
            "message": f"Weaviate not available: {e}",
        }
