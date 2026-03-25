"""Document enrichment step for RAG workflows."""

from steps.enrich.step import EnrichmentStep, ENRICHED_DOCUMENT_SCHEMA
from steps.enrich.enrichers import (
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
    BasicEnricher,
    LLMEnricher,
    AutoEnricher,
    register_enricher,
    get_enricher,
    get_registered_enrichers,
)

__all__ = [
    # Step
    "EnrichmentStep",
    "ENRICHED_DOCUMENT_SCHEMA",
    # Enricher base
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
    # Enricher implementations
    "BasicEnricher",
    "LLMEnricher",
    "AutoEnricher",
    # Registry
    "register_enricher",
    "get_enricher",
    "get_registered_enrichers",
]
