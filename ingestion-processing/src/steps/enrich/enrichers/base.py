"""Base enricher interface for document enrichment."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


class EnricherError(Exception):
    """Error during enricher operation."""

    pass


@dataclass
class GeographyHierarchy:
    """Hierarchical geography context for precise location mapping.

    Enables graph relationships: city → county → state → country
    """
    city: str | None = None
    county: str | None = None
    state: str | None = None
    country: str | None = None
    region: str | None = None  # e.g., "West Coast", "Southwest", "New England"


@dataclass
class EnrichedEntity:
    """An entity extracted from document content.

    Supports canonical name standardization for cross-referencing:
    - name: Original name as found in document
    - canonical_name: Standardized name (e.g., "California" for "CA", "H-1B" for "H1B")
    - aliases: Other common names for this entity

    For geography entities, includes hierarchical context and optional FIPS codes
    for unambiguous US location identification.
    """

    name: str
    type: str  # organization, person, geography, program, metric, agency, legislation, form, date, other
    canonical_name: str | None = None  # Standardized name for cross-referencing
    aliases: list[str] = field(default_factory=list)  # Other names (e.g., ["CA", "Calif."] for California)
    context: str | None = None  # Surrounding context where entity was found
    confidence: float | None = None

    # Geography-specific fields (only for type="geography")
    geography_type: str | None = None  # country, state, region, city, county
    parent_geography: str | None = None  # Immediate parent (e.g., "California" for LA County)
    geography_hierarchy: GeographyHierarchy | None = None  # Full hierarchy for graph relationships
    fips_code: str | None = None  # US FIPS code (2-digit state, 5-digit county, etc.)
    iso_code: str | None = None  # ISO 3166 code for countries/subdivisions (e.g., "US-CA")

    # Program-specific fields (visa categories, government programs)
    program_category: str | None = None  # employment-visa, family-visa, humanitarian, naturalization, etc.


@dataclass
class TemporalScope:
    """Temporal coverage of the document."""

    start_date: str | None = None  # ISO 8601 date
    end_date: str | None = None
    period: str | None = None  # e.g., "Q4 2024", "FY 2024"


@dataclass
class ColumnDescription:
    """Description of a table column for RAG context."""

    column_name: str
    description: str
    data_type: str  # category, numeric, date, text, identifier, etc.
    sample_values: list[str] = field(default_factory=list)


@dataclass
class ExtractedMetric:
    """A metric extracted from table data.

    Captures quantitative measures like approval rates, processing times, counts, etc.
    Used for structured queries and graph relationships.
    """

    name: str  # Canonical metric name: approval_rate, processing_time, petition_count, etc.
    display_name: str  # Human-readable: "Approval Rate", "Processing Time"
    unit: str  # percentage, days, count, currency, etc.
    value: str | None = None  # Actual value if extractable
    context: str | None = None  # What this measures (e.g., "I-130 petitions in FY2024")
    column_name: str | None = None  # Source column in table


@dataclass
class TableEnrichment:
    """Enrichment data for a single table."""

    table_id: str
    description: str
    columns: list[ColumnDescription] = field(default_factory=list)
    key_insights: list[str] = field(default_factory=list)
    relevant_queries: list[str] = field(default_factory=list)
    metrics: list[ExtractedMetric] = field(default_factory=list)  # Metrics found in this table


@dataclass
class SectionEnrichment:
    """Enrichment data for a single section."""

    section_id: str
    summary: str
    key_points: list[str] = field(default_factory=list)
    relevant_queries: list[str] = field(default_factory=list)


@dataclass
class DocumentEnrichment:
    """Document-level enrichment data."""

    summary: str  # Executive summary (2-4 sentences)
    key_topics: list[str] = field(default_factory=list)
    entities: list[EnrichedEntity] = field(default_factory=list)
    temporal_scope: TemporalScope | None = None
    document_type: str | None = None  # statistical_report, form_instructions, etc.
    target_audience: list[str] = field(default_factory=list)
    example_queries: list[str] = field(default_factory=list)


@dataclass
class EnrichmentInfo:
    """Metadata about the enrichment process."""

    enricher: str  # llm, basic, auto
    model: str | None = None  # e.g., claude-3-5-haiku-20241022
    enriched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    processing_time_ms: int = 0
    tokens_used: dict[str, int] = field(default_factory=dict)  # {"input": N, "output": M}
    cost: dict[str, Any] = field(default_factory=dict)  # {"amount": 0.0052, "currency": "USD"}


@dataclass
class EnrichmentResult:
    """Result from an enricher operation.

    Contains all enrichment data following the enriched document schema.
    """

    # Document-level enrichment
    document: DocumentEnrichment

    # Section-level enrichment
    sections: list[SectionEnrichment] = field(default_factory=list)

    # Table-level enrichment
    tables: list[TableEnrichment] = field(default_factory=list)

    # Enrichment metadata
    info: EnrichmentInfo = field(default_factory=lambda: EnrichmentInfo(enricher="unknown"))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result: dict[str, Any] = {
            "document": {
                "summary": self.document.summary,
                "keyTopics": self.document.key_topics,
                "entities": [
                    {
                        "name": e.name,
                        "type": e.type,
                        **({"canonicalName": e.canonical_name} if e.canonical_name else {}),
                        **({"aliases": e.aliases} if e.aliases else {}),
                        **({"context": e.context} if e.context else {}),
                        **({"confidence": e.confidence} if e.confidence else {}),
                        **({"geographyType": e.geography_type} if e.geography_type else {}),
                        **({"parentGeography": e.parent_geography} if e.parent_geography else {}),
                        **({"geographyHierarchy": {
                            **({"city": e.geography_hierarchy.city} if e.geography_hierarchy.city else {}),
                            **({"county": e.geography_hierarchy.county} if e.geography_hierarchy.county else {}),
                            **({"state": e.geography_hierarchy.state} if e.geography_hierarchy.state else {}),
                            **({"country": e.geography_hierarchy.country} if e.geography_hierarchy.country else {}),
                            **({"region": e.geography_hierarchy.region} if e.geography_hierarchy.region else {}),
                        }} if e.geography_hierarchy else {}),
                        **({"fipsCode": e.fips_code} if e.fips_code else {}),
                        **({"isoCode": e.iso_code} if e.iso_code else {}),
                        **({"programCategory": e.program_category} if e.program_category else {}),
                    }
                    for e in self.document.entities
                ],
                **(
                    {
                        "temporalScope": {
                            **({"startDate": self.document.temporal_scope.start_date} if self.document.temporal_scope.start_date else {}),
                            **({"endDate": self.document.temporal_scope.end_date} if self.document.temporal_scope.end_date else {}),
                            **({"period": self.document.temporal_scope.period} if self.document.temporal_scope.period else {}),
                        }
                    }
                    if self.document.temporal_scope
                    else {}
                ),
                **({"documentType": self.document.document_type} if self.document.document_type else {}),
                "targetAudience": self.document.target_audience,
                "exampleQueries": self.document.example_queries,
            },
            "sections": [
                {
                    "sectionId": s.section_id,
                    "summary": s.summary,
                    "keyPoints": s.key_points,
                    "relevantQueries": s.relevant_queries,
                }
                for s in self.sections
            ],
            "tables": [
                {
                    "tableId": t.table_id,
                    "description": t.description,
                    "columns": [
                        {
                            "columnName": c.column_name,
                            "description": c.description,
                            "dataType": c.data_type,
                            "sampleValues": c.sample_values,
                        }
                        for c in t.columns
                    ],
                    "keyInsights": t.key_insights,
                    "relevantQueries": t.relevant_queries,
                    "metrics": [
                        {
                            "name": m.name,
                            "displayName": m.display_name,
                            "unit": m.unit,
                            **({"value": m.value} if m.value else {}),
                            **({"context": m.context} if m.context else {}),
                            **({"columnName": m.column_name} if m.column_name else {}),
                        }
                        for m in t.metrics
                    ],
                }
                for t in self.tables
            ],
            "enrichmentInfo": {
                "enricher": self.info.enricher,
                **({"model": self.info.model} if self.info.model else {}),
                "enrichedAt": self.info.enriched_at.isoformat(),
                "processingTimeMs": self.info.processing_time_ms,
                **({"tokensUsed": self.info.tokens_used} if self.info.tokens_used else {}),
                **({"cost": self.info.cost} if self.info.cost else {}),
            },
        }
        return result


class Enricher(ABC):
    """Abstract base class for document enrichers.

    Enrichers add semantic context, summaries, and structured metadata
    to parsed documents for RAG workflows.

    Subclasses must implement:
    - enricher_type: Class attribute identifying the enricher type
    - enrich(): Method to enrich a parsed document
    """

    # Enricher type identifier (override in subclasses)
    enricher_type: str = "base"

    def __init__(self, **kwargs: Any) -> None:
        """Initialize enricher with optional configuration."""
        self.config = kwargs

    @abstractmethod
    def enrich(
        self,
        parsed_document: dict[str, Any],
    ) -> EnrichmentResult:
        """Enrich a parsed document with semantic context.

        Args:
            parsed_document: Parsed document following parsed-document schema

        Returns:
            EnrichmentResult with document, section, and table enrichments

        Raises:
            EnricherError: If enrichment fails
        """
        pass

    def estimate_tokens(self, text: str) -> int:
        """Estimate token count for text.

        Uses simple approximation of ~4 chars per token.

        Args:
            text: Text to estimate tokens for

        Returns:
            Estimated token count
        """
        return len(text) // 4

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(type={self.enricher_type!r})"
