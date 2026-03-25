"""LLM-based enricher using Anthropic Claude.

Provides high-quality enrichment using Claude for:
- Intelligent document summarization
- Semantic entity extraction with context
- Table and column descriptions
- Query generation
"""

import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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
    ExtractedMetric,
    GeographyHierarchy,
)


# Default model for enrichment
DEFAULT_MODEL = "claude-haiku-4-5"

# Pricing per million tokens
MODEL_PRICING = {
    "claude-haiku-4-5": {"input": 0.80, "output": 4.00},
    "claude-sonnet-4-5": {"input": 3.00, "output": 15.00},
    "claude-3-5-haiku-20241022": {"input": 1.00, "output": 5.00},
    "claude-3-5-sonnet-20241022": {"input": 3.00, "output": 15.00},
}

# Cache directory
CACHE_DIR = Path.home() / ".cache" / "pipeline" / "enrichment"


def _get_ssl_context():
    """Create an SSL context using the system certificate store."""
    import ssl
    try:
        import truststore
        return truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    except ImportError:
        pass
    try:
        import certifi
        return ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        return ssl.create_default_context()


def _get_anthropic_client():
    """Get Anthropic client, lazily imported."""
    try:
        import anthropic
        import httpx

        ssl_context = _get_ssl_context()
        http_client = httpx.Client(verify=ssl_context)
        return anthropic.Anthropic(http_client=http_client)
    except ImportError:
        raise EnricherError(
            "anthropic package not installed. Install with: pip install anthropic"
        )
    except Exception as e:
        raise EnricherError(f"Failed to initialize Anthropic client: {e}")


class LLMEnricher(Enricher):
    """LLM-based enricher using Anthropic Claude.

    Uses Claude's tool use capability for structured output extraction.
    Includes caching to avoid redundant API calls.
    """

    enricher_type = "llm"

    def __init__(self, **kwargs: Any) -> None:
        """Initialize LLM enricher.

        Args:
            **kwargs: Configuration options
                - provider: LLM provider (default: "anthropic")
                - model: Model to use (default: "claude-haiku-4-5")
                - cache_enabled: Enable response caching (default: True)
                - max_tokens: Maximum output tokens (default: 4096)
        """
        super().__init__(**kwargs)
        self.provider = kwargs.get("provider", "anthropic")
        self.model = kwargs.get("model", DEFAULT_MODEL)
        self.cache_enabled = kwargs.get("cache_enabled", True)
        self.max_tokens = kwargs.get("max_tokens", 4096)

        # Validate provider
        if self.provider != "anthropic":
            raise EnricherError(f"Unsupported provider: {self.provider}. Only 'anthropic' is supported.")

    def enrich(self, parsed_document: dict[str, Any]) -> EnrichmentResult:
        """Enrich a parsed document using LLM.

        Args:
            parsed_document: Parsed document following parsed-document schema

        Returns:
            EnrichmentResult with LLM-generated enrichments

        Raises:
            EnricherError: If enrichment fails
        """
        start_time = time.time()

        # Check cache first
        cache_key = self._get_cache_key(parsed_document)
        cached = self._get_cached(cache_key)
        if cached:
            # Update processing time for cache hit
            cached.info.processing_time_ms = int((time.time() - start_time) * 1000)
            return cached

        try:
            # Prepare document content for LLM
            document_text = self._prepare_document_text(parsed_document)

            # Call LLM for enrichment
            result, usage = self._call_llm(document_text, parsed_document)

            processing_time = int((time.time() - start_time) * 1000)

            # Calculate cost
            cost = self._calculate_cost(usage)

            # Update result info
            result.info = EnrichmentInfo(
                enricher="llm",
                model=self.model,
                enriched_at=datetime.now(timezone.utc),
                processing_time_ms=processing_time,
                tokens_used=usage,
                cost=cost,
            )

            # Cache the result
            if self.cache_enabled:
                self._cache_result(cache_key, result)

            return result

        except EnricherError:
            raise
        except Exception as e:
            raise EnricherError(f"LLM enrichment failed: {e}") from e

    def _prepare_document_text(self, parsed_document: dict[str, Any]) -> str:
        """Prepare document text for LLM consumption."""
        parts = []

        # Add metadata
        metadata = parsed_document.get("metadata", {})
        if metadata.get("title"):
            parts.append(f"# {metadata['title']}")
        if metadata.get("publisher"):
            parts.append(f"Publisher: {metadata['publisher']}")

        # Add main content
        content = parsed_document.get("content", {})

        # Add sections if available
        sections = content.get("sections", [])
        if sections:
            parts.append("\n## Document Sections\n")
            for i, section in enumerate(sections[:10]):  # Limit to 10 sections
                title = section.get("title", f"Section {i + 1}")
                parts.append(f"\n### Section {i}: {title}")
                section_content = section.get("content", "")
                if section_content:
                    # Limit section content
                    parts.append(section_content[:2000])
        elif content.get("markdown"):
            parts.append("\n## Content\n")
            parts.append(content["markdown"][:10000])  # Limit content size
        elif content.get("plainText"):
            parts.append("\n## Content\n")
            parts.append(content["plainText"][:10000])

        # Add table summaries
        tables = content.get("tables", [])
        if tables:
            parts.append("\n## Tables\n")
            for i, table in enumerate(tables[:5]):  # Limit to 5 tables
                parts.append(f"\n### Table {i}: {table.get('title', 'Untitled')}")
                headers = table.get("headers", [])
                rows = table.get("rows", [])
                if headers:
                    parts.append(f"Columns: {', '.join(str(h) for h in headers)}")
                parts.append(f"Rows: {len(rows)}")

                # Show sample rows
                if rows:
                    parts.append("Sample data:")
                    for row in rows[:3]:
                        parts.append(f"  {row}")

        return "\n".join(parts)

    def _call_llm(
        self,
        document_text: str,
        parsed_document: dict[str, Any],
    ) -> tuple[EnrichmentResult, dict[str, int]]:
        """Call LLM for document enrichment."""
        client = _get_anthropic_client()

        # Define the enrichment tool with enhanced entity extraction
        enrichment_tool = {
            "name": "extract_enrichment",
            "description": "Extract semantic enrichment from a document for RAG and knowledge graph workflows",
            "input_schema": {
                "type": "object",
                "required": ["summary", "key_topics", "entities", "document_type", "example_queries"],
                "properties": {
                    "summary": {
                        "type": "string",
                        "description": "Executive summary of the document (2-4 sentences)",
                    },
                    "key_topics": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Main topics/themes covered (5-10 items). Use lowercase, hyphenated canonical names (e.g., 'immigration', 'employment-based-visa', 'processing-times')",
                    },
                    "entities": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["name", "type", "canonical_name"],
                            "properties": {
                                "name": {
                                    "type": "string",
                                    "description": "Entity name as it appears in the document",
                                },
                                "type": {
                                    "type": "string",
                                    "enum": ["geography", "program", "agency", "organization", "person", "legislation", "form", "metric", "date", "other"],
                                    "description": "Entity type. Use 'geography' for locations, 'program' for visa categories/government programs",
                                },
                                "canonical_name": {
                                    "type": "string",
                                    "description": "Standardized name for cross-referencing. Examples: 'California' (not 'CA'), 'H-1B' (not 'H1B'), 'USCIS' (not 'U.S. Citizenship...')",
                                },
                                "aliases": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": "Other names/abbreviations for this entity (e.g., ['CA', 'Calif.'] for California)",
                                },
                                "context": {
                                    "type": "string",
                                    "description": "Brief context of how this entity is used in the document",
                                },
                                "geography_type": {
                                    "type": "string",
                                    "enum": ["country", "state", "region", "city", "county"],
                                    "description": "Only for type='geography': the geographic level",
                                },
                                "parent_geography": {
                                    "type": "string",
                                    "description": "Only for type='geography': immediate parent (e.g., 'California' for Los Angeles County)",
                                },
                                "geography_hierarchy": {
                                    "type": "object",
                                    "description": "Only for type='geography': full hierarchy for graph relationships",
                                    "properties": {
                                        "city": {"type": "string", "description": "City name if applicable"},
                                        "county": {"type": "string", "description": "County name (e.g., 'Los Angeles County')"},
                                        "state": {"type": "string", "description": "State name (e.g., 'California')"},
                                        "country": {"type": "string", "description": "Country name (e.g., 'United States')"},
                                        "region": {"type": "string", "description": "Region name (e.g., 'West Coast', 'Southwest')"},
                                    },
                                },
                                "fips_code": {
                                    "type": "string",
                                    "description": "Only for US geography: FIPS code (2-digit state: '06' for CA, 5-digit county: '06037' for LA County)",
                                },
                                "iso_code": {
                                    "type": "string",
                                    "description": "ISO 3166 code for countries (US) or subdivisions (US-CA for California)",
                                },
                                "program_category": {
                                    "type": "string",
                                    "enum": ["employment-visa", "family-visa", "humanitarian", "naturalization", "travel", "other"],
                                    "description": "Only for type='program': category of the program",
                                },
                            },
                        },
                        "description": "Named entities with canonical names for knowledge graph integration",
                    },
                    "temporal_scope": {
                        "type": "object",
                        "properties": {
                            "start_date": {"type": "string", "description": "Start date (ISO 8601, e.g., 2024-01-01)"},
                            "end_date": {"type": "string", "description": "End date (ISO 8601)"},
                            "period": {"type": "string", "description": "Human-readable period (e.g., 'FY 2024 Q1', 'Calendar Year 2023')"},
                            "fiscal_year": {"type": "integer", "description": "Fiscal year if applicable"},
                        },
                    },
                    "document_type": {
                        "type": "string",
                        "enum": ["statistical_report", "form_instructions", "policy_guidance", "data_table", "announcement", "other"],
                        "description": "Type/category of document",
                    },
                    "target_audience": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Intended audience for this document",
                    },
                    "example_queries": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Natural language questions this document can answer (5-10 queries)",
                    },
                    "section_enrichments": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["section_index", "title", "summary"],
                            "properties": {
                                "section_index": {"type": "integer", "description": "0-based index of section"},
                                "title": {"type": "string"},
                                "summary": {"type": "string", "description": "Brief summary of this section (1-2 sentences)"},
                                "key_points": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": "Key points covered in this section",
                                },
                                "relevant_queries": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": "Questions this specific section answers",
                                },
                            },
                        },
                        "description": "Per-section enrichment for granular retrieval",
                    },
                    "table_descriptions": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["table_index", "description"],
                            "properties": {
                                "table_index": {"type": "integer"},
                                "description": {"type": "string"},
                                "key_insights": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                },
                                "column_descriptions": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "column_name": {"type": "string"},
                                            "description": {"type": "string"},
                                            "data_type": {"type": "string"},
                                        },
                                    },
                                },
                                "metrics": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "required": ["name", "display_name", "unit"],
                                        "properties": {
                                            "name": {
                                                "type": "string",
                                                "description": "Canonical metric name (snake_case): approval_rate, processing_time, petition_count, denial_rate, etc.",
                                            },
                                            "display_name": {
                                                "type": "string",
                                                "description": "Human-readable name: 'Approval Rate', 'Processing Time'",
                                            },
                                            "unit": {
                                                "type": "string",
                                                "enum": ["percentage", "days", "count", "currency", "ratio", "other"],
                                            },
                                            "column_name": {
                                                "type": "string",
                                                "description": "Source column containing this metric",
                                            },
                                            "context": {
                                                "type": "string",
                                                "description": "What this metric measures in context",
                                            },
                                        },
                                    },
                                    "description": "Quantitative metrics found in this table (for graph relationships)",
                                },
                            },
                        },
                        "description": "Descriptions for each table in the document",
                    },
                },
            },
        }

        # Build the prompt with canonical naming guidance
        system_prompt = """You are a document enrichment assistant for a government data knowledge graph. Your task is to extract structured semantic information for RAG and graph-based retrieval.

## Core Tasks:
1. Create clear, informative summaries
2. Identify key topics using canonical lowercase-hyphenated names
3. Extract entities with CANONICAL NAMES for cross-referencing
4. Determine temporal scope with fiscal/calendar year info
5. Classify document type
6. Identify target audience
7. Generate natural language queries this document answers
8. Describe tables with metrics extraction
9. Summarize each document section

## Canonical Naming Rules (CRITICAL for knowledge graph):

### Geography (IMPORTANT - include full hierarchy):
- Use full names: "California" (not "CA"), "United States" (not "US")
- Include geography_type: country, state, county, city, region
- Include parent_geography: immediate parent location
- Include geography_hierarchy with ALL levels:
  - For cities: {city, county, state, country}
  - For counties: {county, state, country}
  - For states: {state, country}
- Include FIPS codes for US locations when known:
  - States: 2-digit (e.g., "06" for California)
  - Counties: 5-digit (e.g., "06037" for Los Angeles County)
- Include ISO codes: "US" for USA, "US-CA" for California

### Programs & Visa Categories:
- Use hyphenated form numbers: "H-1B" (not "H1B", "H-1b")
- Use standard abbreviations: "TPS", "DACA", "EB-5"
- Full form names: "I-130", "I-485", "I-765"
- Include program_category

### Agencies:
- Use standard abbreviations: "USCIS", "DHS", "DOL", "DOS"
- Not full names unless no abbreviation exists

### Metrics (for tables):
- Use snake_case: approval_rate, processing_time, petition_count
- Identify unit type: percentage, days, count, currency

### Topics:
- Use lowercase hyphenated: "employment-based-immigration", "family-visa", "processing-times"

## Example Entity Extractions:
- "CA" → canonical_name: "California", type: "geography", geography_type: "state", aliases: ["CA", "Calif."]
- "H1B visa" → canonical_name: "H-1B", type: "program", program_category: "employment-visa", aliases: ["H1B", "H-1B visa"]
- "approval rate of 85%" → metric with name: "approval_rate", unit: "percentage"

Be thorough in entity extraction. Include ALL geographic locations, programs, and agencies mentioned."""

        user_message = f"""Analyze this document and extract enrichment metadata:

{document_text}

Use the extract_enrichment tool to provide your analysis."""

        # Call the API
        response = client.messages.create(
            model=self.model,
            max_tokens=self.max_tokens,
            system=system_prompt,
            tools=[enrichment_tool],
            tool_choice={"type": "tool", "name": "extract_enrichment"},
            messages=[{"role": "user", "content": user_message}],
        )

        # Extract usage
        usage = {
            "input": response.usage.input_tokens,
            "output": response.usage.output_tokens,
        }

        # Parse tool use response
        for block in response.content:
            if block.type == "tool_use" and block.name == "extract_enrichment":
                return self._parse_llm_response(block.input, parsed_document), usage

        raise EnricherError("LLM did not return expected tool use response")

    def _parse_llm_response(
        self,
        response: dict[str, Any],
        parsed_document: dict[str, Any],
    ) -> EnrichmentResult:
        """Parse LLM response into EnrichmentResult with canonical names."""
        # Parse entities with enhanced fields
        entities = []
        for e in response.get("entities", []):
            # Parse geography hierarchy if present
            geo_hierarchy = None
            geo_hierarchy_data = e.get("geography_hierarchy")
            if geo_hierarchy_data:
                geo_hierarchy = GeographyHierarchy(
                    city=geo_hierarchy_data.get("city"),
                    county=geo_hierarchy_data.get("county"),
                    state=geo_hierarchy_data.get("state"),
                    country=geo_hierarchy_data.get("country"),
                    region=geo_hierarchy_data.get("region"),
                )

            entity = EnrichedEntity(
                name=e.get("name", ""),
                type=e.get("type", "other"),
                canonical_name=e.get("canonical_name"),
                aliases=e.get("aliases", []),
                context=e.get("context"),
                geography_type=e.get("geography_type"),
                parent_geography=e.get("parent_geography"),
                geography_hierarchy=geo_hierarchy,
                fips_code=e.get("fips_code"),
                iso_code=e.get("iso_code"),
                program_category=e.get("program_category"),
            )
            entities.append(entity)

        # Parse temporal scope
        temporal_data = response.get("temporal_scope", {})
        temporal_scope = None
        if temporal_data:
            temporal_scope = TemporalScope(
                start_date=temporal_data.get("start_date"),
                end_date=temporal_data.get("end_date"),
                period=temporal_data.get("period"),
            )

        # Create document enrichment
        document = DocumentEnrichment(
            summary=response.get("summary", ""),
            key_topics=response.get("key_topics", []),
            entities=entities,
            temporal_scope=temporal_scope,
            document_type=response.get("document_type"),
            target_audience=response.get("target_audience", []),
            example_queries=response.get("example_queries", []),
        )

        # Parse section enrichments
        sections = []
        for section_data in response.get("section_enrichments", []):
            section_index = section_data.get("section_index", 0)
            sections.append(SectionEnrichment(
                section_id=f"section-{section_index}",
                summary=section_data.get("summary", ""),
                key_points=section_data.get("key_points", []),
                relevant_queries=section_data.get("relevant_queries", []),
            ))

        # Parse table descriptions with metrics
        tables = []
        content = parsed_document.get("content", {})
        doc_tables = content.get("tables", [])

        for table_desc in response.get("table_descriptions", []):
            table_index = table_desc.get("table_index", 0)
            table_id = f"table-{table_index}"

            # Get column info from parsed document
            original_table = doc_tables[table_index] if table_index < len(doc_tables) else {}
            headers = original_table.get("headers", [])
            rows = original_table.get("rows", [])

            # Parse column descriptions
            columns = []
            for col_desc in table_desc.get("column_descriptions", []):
                col_name = col_desc.get("column_name", "")
                # Get sample values from actual data
                col_index = -1
                try:
                    col_index = headers.index(col_name) if col_name in headers else -1
                except (ValueError, AttributeError):
                    pass

                sample_values = []
                if col_index >= 0:
                    for row in rows[:5]:
                        if col_index < len(row) and row[col_index] is not None:
                            sample_values.append(str(row[col_index]))

                columns.append(ColumnDescription(
                    column_name=col_name,
                    description=col_desc.get("description", ""),
                    data_type=col_desc.get("data_type", "text"),
                    sample_values=sample_values,
                ))

            # Parse metrics
            metrics = []
            for metric_data in table_desc.get("metrics", []):
                metrics.append(ExtractedMetric(
                    name=metric_data.get("name", ""),
                    display_name=metric_data.get("display_name", ""),
                    unit=metric_data.get("unit", "other"),
                    value=metric_data.get("value"),
                    context=metric_data.get("context"),
                    column_name=metric_data.get("column_name"),
                ))

            tables.append(TableEnrichment(
                table_id=table_id,
                description=table_desc.get("description", ""),
                columns=columns,
                key_insights=table_desc.get("key_insights", []),
                relevant_queries=[],
                metrics=metrics,
            ))

        return EnrichmentResult(
            document=document,
            sections=sections,
            tables=tables,
            info=EnrichmentInfo(enricher="llm"),
        )

    def _calculate_cost(self, usage: dict[str, int]) -> dict[str, Any]:
        """Calculate cost based on token usage."""
        pricing = MODEL_PRICING.get(self.model, MODEL_PRICING[DEFAULT_MODEL])

        input_cost = (usage.get("input", 0) / 1_000_000) * pricing["input"]
        output_cost = (usage.get("output", 0) / 1_000_000) * pricing["output"]

        return {
            "amount": round(input_cost + output_cost, 6),
            "currency": "USD",
        }

    def _get_cache_key(self, parsed_document: dict[str, Any]) -> str:
        """Generate cache key for a document."""
        # Use checksum + model version for cache key
        source = parsed_document.get("source", {})
        checksum = source.get("checksum", "")

        if not checksum:
            # Generate checksum from content
            content = json.dumps(parsed_document.get("content", {}), sort_keys=True)
            checksum = hashlib.sha256(content.encode()).hexdigest()[:16]

        return f"{checksum}_{self.model}"

    def _get_cached(self, cache_key: str) -> EnrichmentResult | None:
        """Get cached enrichment result."""
        if not self.cache_enabled:
            return None

        cache_file = CACHE_DIR / f"{cache_key}.json"
        if not cache_file.exists():
            return None

        try:
            with open(cache_file) as f:
                data = json.load(f)
            return self._dict_to_result(data)
        except Exception:
            return None

    def _cache_result(self, cache_key: str, result: EnrichmentResult) -> None:
        """Cache enrichment result."""
        if not self.cache_enabled:
            return

        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cache_file = CACHE_DIR / f"{cache_key}.json"

        try:
            with open(cache_file, "w") as f:
                json.dump(result.to_dict(), f, indent=2)
        except Exception:
            pass  # Silently fail on cache write errors

    def _dict_to_result(self, data: dict[str, Any]) -> EnrichmentResult:
        """Convert cached dict back to EnrichmentResult."""
        doc_data = data.get("document", {})

        # Parse entities with enhanced fields
        entities = []
        for e in doc_data.get("entities", []):
            # Parse geography hierarchy from cached data (camelCase)
            geo_hierarchy = None
            geo_hierarchy_data = e.get("geographyHierarchy")
            if geo_hierarchy_data:
                geo_hierarchy = GeographyHierarchy(
                    city=geo_hierarchy_data.get("city"),
                    county=geo_hierarchy_data.get("county"),
                    state=geo_hierarchy_data.get("state"),
                    country=geo_hierarchy_data.get("country"),
                    region=geo_hierarchy_data.get("region"),
                )

            entities.append(EnrichedEntity(
                name=e.get("name", ""),
                type=e.get("type", "other"),
                canonical_name=e.get("canonicalName"),
                aliases=e.get("aliases", []),
                context=e.get("context"),
                confidence=e.get("confidence"),
                geography_type=e.get("geographyType"),
                parent_geography=e.get("parentGeography"),
                geography_hierarchy=geo_hierarchy,
                fips_code=e.get("fipsCode"),
                iso_code=e.get("isoCode"),
                program_category=e.get("programCategory"),
            ))

        # Parse temporal scope
        temporal_data = doc_data.get("temporalScope", {})
        temporal_scope = None
        if temporal_data:
            temporal_scope = TemporalScope(
                start_date=temporal_data.get("startDate"),
                end_date=temporal_data.get("endDate"),
                period=temporal_data.get("period"),
            )

        document = DocumentEnrichment(
            summary=doc_data.get("summary", ""),
            key_topics=doc_data.get("keyTopics", []),
            entities=entities,
            temporal_scope=temporal_scope,
            document_type=doc_data.get("documentType"),
            target_audience=doc_data.get("targetAudience", []),
            example_queries=doc_data.get("exampleQueries", []),
        )

        # Parse sections
        sections = [
            SectionEnrichment(
                section_id=s.get("sectionId", ""),
                summary=s.get("summary", ""),
                key_points=s.get("keyPoints", []),
                relevant_queries=s.get("relevantQueries", []),
            )
            for s in data.get("sections", [])
        ]

        # Parse tables with metrics
        tables = []
        for t in data.get("tables", []):
            # Parse metrics
            metrics = [
                ExtractedMetric(
                    name=m.get("name", ""),
                    display_name=m.get("displayName", ""),
                    unit=m.get("unit", "other"),
                    value=m.get("value"),
                    context=m.get("context"),
                    column_name=m.get("columnName"),
                )
                for m in t.get("metrics", [])
            ]

            tables.append(TableEnrichment(
                table_id=t.get("tableId", ""),
                description=t.get("description", ""),
                columns=[
                    ColumnDescription(
                        column_name=c.get("columnName", ""),
                        description=c.get("description", ""),
                        data_type=c.get("dataType", "text"),
                        sample_values=c.get("sampleValues", []),
                    )
                    for c in t.get("columns", [])
                ],
                key_insights=t.get("keyInsights", []),
                relevant_queries=t.get("relevantQueries", []),
                metrics=metrics,
            ))

        # Parse info
        info_data = data.get("enrichmentInfo", {})
        enriched_at = info_data.get("enrichedAt")
        if enriched_at:
            enriched_at = datetime.fromisoformat(enriched_at.replace("Z", "+00:00"))
        else:
            enriched_at = datetime.now(timezone.utc)

        info = EnrichmentInfo(
            enricher=info_data.get("enricher", "llm"),
            model=info_data.get("model"),
            enriched_at=enriched_at,
            processing_time_ms=info_data.get("processingTimeMs", 0),
            tokens_used=info_data.get("tokensUsed", {}),
            cost=info_data.get("cost", {}),
        )

        return EnrichmentResult(
            document=document,
            sections=sections,
            tables=tables,
            info=info,
        )
