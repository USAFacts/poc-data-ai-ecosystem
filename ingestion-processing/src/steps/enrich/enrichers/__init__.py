"""Document enrichers for semantic context extraction.

Enrichers add semantic context, summaries, and structured metadata
to parsed documents for RAG (Retrieval Augmented Generation) workflows.
"""

from steps.enrich.enrichers.base import (
    Enricher,
    EnricherError,
    EnrichmentResult,
    EnrichmentInfo,
    DocumentEnrichment,
    SectionEnrichment,
    TableEnrichment,
    EnrichedEntity,
    TemporalScope,
    ColumnDescription,
)
from steps.enrich.enrichers.registry import (
    register_enricher,
    get_enricher,
    get_enricher_class,
    get_registered_enrichers,
)
from steps.enrich.enrichers.basic import BasicEnricher
from steps.enrich.enrichers.llm import LLMEnricher
from steps.enrich.enrichers.auto import AutoEnricher

__all__ = [
    # Base classes
    "Enricher",
    "EnricherError",
    "EnrichmentResult",
    "EnrichmentInfo",
    "DocumentEnrichment",
    "SectionEnrichment",
    "TableEnrichment",
    "EnrichedEntity",
    "TemporalScope",
    "ColumnDescription",
    # Registry
    "register_enricher",
    "get_enricher",
    "get_enricher_class",
    "get_registered_enrichers",
    # Implementations
    "BasicEnricher",
    "LLMEnricher",
    "AutoEnricher",
]
