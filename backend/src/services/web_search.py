"""Web search service using Firecrawl for .gov and usafacts.org sources."""

import os
import logging
from typing import Any

logger = logging.getLogger(__name__)

_client = None


def _get_client():
    global _client
    if _client is None:
        api_key = os.getenv("FIRECRAWL_API_KEY")
        if not api_key:
            logger.warning("[web_search] FIRECRAWL_API_KEY not set")
            return None
        try:
            from firecrawl import FirecrawlApp
            _client = FirecrawlApp(api_key=api_key)
        except Exception as e:
            logger.warning(f"[web_search] Failed to initialize Firecrawl: {e}")
            return None
    return _client


def _parse_search_response(response, limit: int) -> list[dict[str, Any]]:
    """Parse a Firecrawl search response into a list of result dicts."""
    items = []
    if hasattr(response, "web") and response.web:
        items = response.web
    elif hasattr(response, "data") and response.data:
        items = response.data
    elif isinstance(response, list):
        items = response

    results = []
    for item in items[:limit]:
        if hasattr(item, "url"):
            url = item.url or ""
            title = item.title or ""
            content = item.description or ""
        else:
            url = item.get("url", "")
            title = item.get("title", "")
            content = item.get("description", "") or item.get("content", "")

        source_domain = (
            "usafacts.org" if "usafacts" in url
            else "gov" if ".gov" in url
            else "other"
        )

        results.append({
            "title": title,
            "url": url,
            "content": content[:2000],
            "source_domain": source_domain,
        })

    return results


def search_gov_sources(query: str, limit: int = 8) -> list[dict[str, Any]]:
    """Search .gov and usafacts.org for relevant content.

    Searches both sources independently and interleaves results
    to ensure both are represented in the output.

    Returns list of dicts with: title, url, content, source_domain
    """
    client = _get_client()
    if not client:
        return []

    # Search each source independently
    per_source: dict[str, list[dict[str, Any]]] = {}

    for site_filter in ["site:usafacts.org", "site:*.gov"]:
        try:
            search_query = f"{query} {site_filter}"
            response = client.search(query=search_query, limit=limit)
            source_key = "usafacts" if "usafacts" in site_filter else "gov"
            per_source[source_key] = _parse_search_response(response, limit)
        except Exception as e:
            logger.warning(f"[web_search] Search failed for '{site_filter}': {e}")

    # Interleave results: ensure both sources are represented
    # Allocate slots proportionally, but guarantee at least 2 per source if available
    usafacts_results = per_source.get("usafacts", [])
    gov_results = per_source.get("gov", [])

    if not usafacts_results and not gov_results:
        return []

    # Guarantee minimum representation, then fill remaining slots
    min_per_source = min(2, limit // 2)
    combined = []

    # Take guaranteed slots from each
    uf_guaranteed = usafacts_results[:min_per_source]
    gov_guaranteed = gov_results[:min_per_source]
    combined.extend(uf_guaranteed)
    combined.extend(gov_guaranteed)

    # Fill remaining slots from both sources (round-robin by rank)
    remaining = limit - len(combined)
    uf_rest = usafacts_results[min_per_source:]
    gov_rest = gov_results[min_per_source:]

    # Interleave remaining
    idx_uf = idx_gov = 0
    while remaining > 0 and (idx_uf < len(uf_rest) or idx_gov < len(gov_rest)):
        if idx_gov < len(gov_rest):
            combined.append(gov_rest[idx_gov])
            idx_gov += 1
            remaining -= 1
        if remaining > 0 and idx_uf < len(uf_rest):
            combined.append(uf_rest[idx_uf])
            idx_uf += 1
            remaining -= 1

    return combined[:limit]


def is_available() -> bool:
    """Check if web search is configured."""
    return bool(os.getenv("FIRECRAWL_API_KEY"))
