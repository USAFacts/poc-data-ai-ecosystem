"""Basic rule-based enricher.

Provides enrichment without LLM API costs using:
- Word frequency for keyword extraction
- Regex patterns for entity detection
- Column type inference for tables
"""

import re
import time
from collections import Counter
from datetime import datetime, timezone
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
)


# Common stop words to filter from keywords
STOP_WORDS = {
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "as", "is", "was", "are", "were", "been",
    "be", "have", "has", "had", "do", "does", "did", "will", "would",
    "could", "should", "may", "might", "must", "shall", "can", "this",
    "that", "these", "those", "it", "its", "they", "them", "their",
    "we", "our", "us", "you", "your", "he", "she", "him", "her", "his",
    "not", "no", "yes", "all", "any", "some", "each", "every", "both",
    "other", "such", "than", "then", "so", "if", "when", "where", "who",
    "what", "which", "how", "why", "just", "also", "only", "more", "most",
    "very", "too", "here", "there", "now", "about", "into", "over", "after",
    "before", "between", "through", "during", "under", "again", "further",
    "once", "data", "table", "total", "number", "percent", "year", "years",
}

# Regex patterns for entity extraction
ENTITY_PATTERNS = {
    "form": re.compile(r"\b([A-Z]-\d{2,4}[A-Z]?)\b"),  # e.g., I-130, I-485, N-400
    "agency": re.compile(
        r"\b(USCIS|DHS|DOJ|FBI|ICE|CBP|DOS|DOL|SSA|IRS|EPA|FDA|CDC|NIH|FEMA|"
        r"Department of (?:Homeland Security|Justice|Labor|State|Defense|Treasury)|"
        r"U\.?S\.? Citizenship and Immigration Services)\b",
        re.IGNORECASE,
    ),
    "date": re.compile(
        r"\b(\d{1,2}/\d{1,2}/\d{2,4}|\d{4}-\d{2}-\d{2}|"
        r"(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}|"
        r"(?:Q[1-4]|FY)\s*\d{4})\b",
        re.IGNORECASE,
    ),
    "money": re.compile(r"\$[\d,]+(?:\.\d{2})?(?:\s*(?:million|billion|M|B))?\b"),
    "percentage": re.compile(r"\b\d+(?:\.\d+)?%\b"),
    "legislation": re.compile(
        r"\b(?:Public Law|P\.L\.|Act of \d{4}|Immigration (?:and Nationality )?Act|"
        r"INA|8 U\.S\.C\.|8 CFR)\b",
        re.IGNORECASE,
    ),
}

# Patterns for document type detection
DOCUMENT_TYPE_PATTERNS = {
    "statistical_report": [
        r"statistic", r"quarterly", r"annual report", r"fiscal year",
        r"data release", r"metrics", r"performance",
    ],
    "form_instructions": [
        r"instructions", r"how to file", r"filing fee", r"form\s+[a-z]-\d+",
    ],
    "policy_guidance": [
        r"policy", r"guidance", r"memorandum", r"directive", r"regulation",
    ],
    "data_table": [
        r"dataset", r"data table", r"appendix", r"raw data",
    ],
}

# Fiscal year quarter patterns
QUARTER_PATTERNS = [
    (re.compile(r"Q([1-4])\s*(?:FY\s*)?(\d{4})", re.IGNORECASE), lambda m: (m.group(2), m.group(1))),
    (re.compile(r"FY\s*(\d{4})\s*Q([1-4])", re.IGNORECASE), lambda m: (m.group(1), m.group(2))),
    (re.compile(r"(?:first|second|third|fourth)\s+quarter\s+(?:of\s+)?(\d{4})", re.IGNORECASE),
     lambda m: (m.group(1), {"first": "1", "second": "2", "third": "3", "fourth": "4"}.get(m.group(0).split()[0].lower(), "1"))),
]


class BasicEnricher(Enricher):
    """Rule-based enricher for cost-effective document enrichment.

    Uses heuristics, regex patterns, and statistical methods to extract:
    - Keywords via word frequency (TF)
    - Named entities via regex patterns
    - Document type classification
    - Temporal scope from date patterns
    - Column type inference for tables
    """

    enricher_type = "basic"

    def __init__(self, **kwargs: Any) -> None:
        """Initialize basic enricher.

        Args:
            **kwargs: Configuration options
                - max_keywords: Maximum keywords to extract (default: 10)
                - min_word_length: Minimum word length for keywords (default: 3)
        """
        super().__init__(**kwargs)
        self.max_keywords = kwargs.get("max_keywords", 10)
        self.min_word_length = kwargs.get("min_word_length", 3)

    def enrich(self, parsed_document: dict[str, Any]) -> EnrichmentResult:
        """Enrich a parsed document using rule-based extraction.

        Args:
            parsed_document: Parsed document following parsed-document schema

        Returns:
            EnrichmentResult with extracted enrichments

        Raises:
            EnricherError: If enrichment fails
        """
        start_time = time.time()

        try:
            content = parsed_document.get("content", {})
            metadata = parsed_document.get("metadata", {})

            # Extract full text for analysis
            full_text = self._get_full_text(content)

            # Extract document-level enrichment
            document_enrichment = self._enrich_document(full_text, metadata, content)

            # Extract section-level enrichments
            section_enrichments = self._enrich_sections(content)

            # Extract table-level enrichments
            table_enrichments = self._enrich_tables(content)

            processing_time = int((time.time() - start_time) * 1000)

            return EnrichmentResult(
                document=document_enrichment,
                sections=section_enrichments,
                tables=table_enrichments,
                info=EnrichmentInfo(
                    enricher="basic",
                    model=None,
                    enriched_at=datetime.now(timezone.utc),
                    processing_time_ms=processing_time,
                ),
            )

        except Exception as e:
            raise EnricherError(f"Basic enrichment failed: {e}") from e

    def _get_full_text(self, content: dict[str, Any]) -> str:
        """Extract full text from content for analysis."""
        parts = []

        # Add markdown or plain text
        if content.get("markdown"):
            parts.append(content["markdown"])
        elif content.get("plainText"):
            parts.append(content["plainText"])

        # Add section content
        for section in content.get("sections", []):
            if section.get("title"):
                parts.append(section["title"])
            if section.get("content"):
                parts.append(section["content"])

        return "\n".join(parts)

    def _enrich_document(
        self,
        text: str,
        metadata: dict[str, Any],
        content: dict[str, Any],
    ) -> DocumentEnrichment:
        """Extract document-level enrichment."""
        # Extract keywords
        keywords = self._extract_keywords(text)

        # Extract entities
        entities = self._extract_entities(text)

        # Detect document type
        doc_type = self._detect_document_type(text)

        # Extract temporal scope
        temporal = self._extract_temporal_scope(text)

        # Generate basic summary from title and first content
        summary = self._generate_summary(metadata, content)

        # Infer target audience
        audience = self._infer_audience(text, doc_type)

        # Generate example queries
        queries = self._generate_queries(keywords, entities, metadata)

        return DocumentEnrichment(
            summary=summary,
            key_topics=keywords,
            entities=entities,
            temporal_scope=temporal,
            document_type=doc_type,
            target_audience=audience,
            example_queries=queries,
        )

    def _extract_keywords(self, text: str) -> list[str]:
        """Extract keywords using word frequency."""
        # Tokenize and clean
        words = re.findall(r"\b[a-zA-Z]+\b", text.lower())

        # Filter stop words and short words
        words = [
            w for w in words
            if w not in STOP_WORDS and len(w) >= self.min_word_length
        ]

        # Count frequencies
        counter = Counter(words)

        # Get top keywords
        return [word for word, _ in counter.most_common(self.max_keywords)]

    def _extract_entities(self, text: str) -> list[EnrichedEntity]:
        """Extract named entities using regex patterns."""
        entities = []
        seen = set()

        for entity_type, pattern in ENTITY_PATTERNS.items():
            for match in pattern.finditer(text):
                value = match.group(0)
                # Normalize for deduplication
                normalized = value.upper() if entity_type == "agency" else value

                if normalized not in seen:
                    seen.add(normalized)

                    # Get context (surrounding text)
                    start = max(0, match.start() - 50)
                    end = min(len(text), match.end() + 50)
                    context = text[start:end].strip()

                    entities.append(EnrichedEntity(
                        name=value,
                        type=entity_type,
                        context=f"...{context}..." if start > 0 or end < len(text) else context,
                    ))

        return entities[:20]  # Limit to 20 entities

    def _detect_document_type(self, text: str) -> str | None:
        """Detect document type from content patterns."""
        text_lower = text.lower()

        scores = {}
        for doc_type, patterns in DOCUMENT_TYPE_PATTERNS.items():
            score = sum(1 for p in patterns if re.search(p, text_lower))
            if score > 0:
                scores[doc_type] = score

        if scores:
            return max(scores, key=scores.get)
        return None

    def _extract_temporal_scope(self, text: str) -> TemporalScope | None:
        """Extract temporal scope from date patterns."""
        # Try to find fiscal year/quarter patterns
        for pattern, extractor in QUARTER_PATTERNS:
            match = pattern.search(text)
            if match:
                year, quarter = extractor(match)
                # Calculate quarter date range
                quarter_int = int(quarter)
                start_month = (quarter_int - 1) * 3 + 1
                end_month = start_month + 2

                return TemporalScope(
                    start_date=f"{year}-{start_month:02d}-01",
                    end_date=f"{year}-{end_month:02d}-{'30' if end_month in [4, 6, 9, 11] else '31' if end_month != 2 else '28'}",
                    period=f"Q{quarter} {year}",
                )

        # Try to find year ranges
        year_pattern = re.compile(r"\b(20\d{2})\s*(?:-|to|through)\s*(20\d{2})\b")
        match = year_pattern.search(text)
        if match:
            return TemporalScope(
                start_date=f"{match.group(1)}-01-01",
                end_date=f"{match.group(2)}-12-31",
            )

        # Single year
        year_pattern = re.compile(r"\bFY\s*(20\d{2})\b|\b(20\d{2})\s+(?:Annual|Report)\b", re.IGNORECASE)
        match = year_pattern.search(text)
        if match:
            year = match.group(1) or match.group(2)
            return TemporalScope(
                start_date=f"{year}-01-01",
                end_date=f"{year}-12-31",
                period=f"FY {year}",
            )

        return None

    def _generate_summary(
        self,
        metadata: dict[str, Any],
        content: dict[str, Any],
    ) -> str:
        """Generate a basic summary from available content."""
        parts = []

        # Use title
        title = metadata.get("title", "")
        if title:
            parts.append(title)

        # Use publisher
        publisher = metadata.get("publisher", "")
        if publisher:
            parts.append(f"Published by {publisher}.")

        # Add content description
        tables = content.get("tables", [])
        sections = content.get("sections", [])

        if tables:
            parts.append(f"Contains {len(tables)} data table(s).")
        if sections:
            parts.append(f"Organized into {len(sections)} section(s).")

        # Get first section for context
        if sections and sections[0].get("content"):
            first_content = sections[0]["content"][:200]
            if len(sections[0]["content"]) > 200:
                first_content += "..."
            parts.append(first_content)

        return " ".join(parts) if parts else "Document content extracted for analysis."

    def _infer_audience(self, text: str, doc_type: str | None) -> list[str]:
        """Infer target audience from content."""
        audiences = []

        text_lower = text.lower()

        # Check for research-oriented content
        if any(term in text_lower for term in ["methodology", "statistical", "analysis", "data"]):
            audiences.append("researchers")

        # Check for policy content
        if any(term in text_lower for term in ["policy", "regulation", "compliance", "requirement"]):
            audiences.append("policymakers")

        # Check for practitioner content
        if any(term in text_lower for term in ["filing", "application", "form", "instructions", "how to"]):
            audiences.append("practitioners")

        # Check for general public content
        if any(term in text_lower for term in ["public", "citizens", "applicants", "individuals"]):
            audiences.append("general_public")

        # Default based on document type
        if not audiences:
            if doc_type == "statistical_report":
                audiences = ["researchers", "analysts"]
            elif doc_type == "form_instructions":
                audiences = ["applicants", "practitioners"]
            elif doc_type == "policy_guidance":
                audiences = ["policymakers", "legal_professionals"]
            else:
                audiences = ["general"]

        return audiences[:3]

    def _generate_queries(
        self,
        keywords: list[str],
        entities: list[EnrichedEntity],
        metadata: dict[str, Any],
    ) -> list[str]:
        """Generate example queries this document could answer."""
        queries = []

        # Form-based queries
        forms = [e for e in entities if e.type == "form"]
        for form in forms[:2]:
            queries.append(f"What is the status of {form.name} applications?")
            queries.append(f"How many {form.name} forms were processed?")

        # Topic-based queries
        if keywords:
            topic = keywords[0]
            queries.append(f"What are the latest {topic} statistics?")

        # Publisher-based queries
        publisher = metadata.get("publisher", "")
        if publisher:
            queries.append(f"What data does {publisher} publish?")

        return queries[:5]

    def _enrich_sections(self, content: dict[str, Any]) -> list[SectionEnrichment]:
        """Enrich individual sections."""
        enrichments = []

        for i, section in enumerate(content.get("sections", [])):
            section_id = section.get("id", f"section-{i + 1}")
            section_content = section.get("content", "")
            section_title = section.get("title", "")

            # Generate section summary
            summary = section_title or section_content[:100]
            if len(section_content) > 100 and not section_title:
                summary += "..."

            # Extract key points (sentences with important keywords)
            key_points = self._extract_key_points(section_content)

            # Generate relevant queries
            queries = []
            if section_title:
                queries.append(f"What does the {section_title} section cover?")

            enrichments.append(SectionEnrichment(
                section_id=section_id,
                summary=summary,
                key_points=key_points,
                relevant_queries=queries,
            ))

        return enrichments

    def _extract_key_points(self, text: str) -> list[str]:
        """Extract key points from text."""
        # Split into sentences
        sentences = re.split(r"[.!?]+", text)
        sentences = [s.strip() for s in sentences if len(s.strip()) > 20]

        # Score sentences by keyword presence
        keywords = self._extract_keywords(text)[:5]

        scored = []
        for sentence in sentences:
            sentence_lower = sentence.lower()
            score = sum(1 for kw in keywords if kw in sentence_lower)
            if score > 0:
                scored.append((score, sentence))

        # Return top scoring sentences
        scored.sort(reverse=True)
        return [s for _, s in scored[:3]]

    def _enrich_tables(self, content: dict[str, Any]) -> list[TableEnrichment]:
        """Enrich individual tables with column descriptions and insights."""
        enrichments = []

        for i, table in enumerate(content.get("tables", [])):
            table_id = table.get("id", f"table-{i + 1}")
            headers = table.get("headers", [])
            rows = table.get("rows", [])
            title = table.get("title", "")

            # Generate table description
            description = title or f"Table with {len(headers)} columns and {len(rows)} rows"

            # Analyze columns
            columns = []
            for j, header in enumerate(headers):
                col_data = self._analyze_column(header, rows, j)
                columns.append(col_data)

            # Generate key insights
            insights = self._generate_table_insights(headers, rows)

            # Generate relevant queries
            queries = []
            if headers:
                queries.append(f"What is the breakdown by {headers[0]}?")

            enrichments.append(TableEnrichment(
                table_id=table_id,
                description=description,
                columns=columns,
                key_insights=insights,
                relevant_queries=queries,
            ))

        return enrichments

    def _analyze_column(
        self,
        header: str,
        rows: list[list[Any]],
        col_index: int,
    ) -> ColumnDescription:
        """Analyze a table column to determine type and samples."""
        values = []
        for row in rows:
            if col_index < len(row):
                val = row[col_index]
                if val is not None and str(val).strip():
                    values.append(val)

        # Determine data type
        data_type = self._infer_data_type(values)

        # Get sample values
        unique_values = list(dict.fromkeys(str(v) for v in values[:10]))
        sample_values = unique_values[:5]

        # Generate description
        description = self._generate_column_description(header, data_type, len(set(str(v) for v in values)))

        return ColumnDescription(
            column_name=header,
            description=description,
            data_type=data_type,
            sample_values=sample_values,
        )

    def _infer_data_type(self, values: list[Any]) -> str:
        """Infer the data type of column values."""
        if not values:
            return "text"

        # Sample values for type checking
        sample = values[:20]

        # Check for numeric
        numeric_count = 0
        for v in sample:
            try:
                float(str(v).replace(",", "").replace("%", "").replace("$", ""))
                numeric_count += 1
            except (ValueError, TypeError):
                pass

        if numeric_count > len(sample) * 0.8:
            # Check for percentage or currency
            str_vals = [str(v) for v in sample]
            if any("%" in v for v in str_vals):
                return "percentage"
            if any("$" in v for v in str_vals):
                return "currency"
            return "numeric"

        # Check for date patterns
        date_count = sum(
            1 for v in sample
            if re.match(r"\d{1,2}/\d{1,2}/\d{2,4}|\d{4}-\d{2}-\d{2}", str(v))
        )
        if date_count > len(sample) * 0.5:
            return "date"

        # Check for categorical (low cardinality)
        unique_ratio = len(set(str(v) for v in values)) / len(values) if values else 1
        if unique_ratio < 0.1:
            return "category"

        # Check for identifiers (codes, IDs)
        if re.match(r"^[A-Z]-\d+", str(sample[0])):
            return "identifier"

        return "text"

    def _generate_column_description(
        self,
        header: str,
        data_type: str,
        unique_count: int,
    ) -> str:
        """Generate a description for a column."""
        header_lower = header.lower()

        # Common column name patterns
        if any(term in header_lower for term in ["form", "type", "category", "class"]):
            return f"Classification or type field with {unique_count} unique values"
        if any(term in header_lower for term in ["count", "number", "total", "quantity"]):
            return f"Numeric count or quantity measure"
        if any(term in header_lower for term in ["rate", "percent", "ratio"]):
            return f"Rate or percentage value"
        if any(term in header_lower for term in ["date", "time", "period", "year", "quarter"]):
            return f"Temporal field indicating time period"
        if any(term in header_lower for term in ["amount", "value", "cost", "fee"]):
            return f"Monetary or value field"
        if any(term in header_lower for term in ["status", "result", "outcome"]):
            return f"Status or outcome classification"

        return f"{data_type.capitalize()} field from source data"

    def _generate_table_insights(
        self,
        headers: list[str],
        rows: list[list[Any]],
    ) -> list[str]:
        """Generate key insights about a table."""
        insights = []

        if not rows:
            return insights

        # Basic stats
        insights.append(f"Contains {len(rows)} data rows across {len(headers)} columns")

        # Find numeric columns and compute simple stats
        for i, header in enumerate(headers):
            values = []
            for row in rows:
                if i < len(row):
                    try:
                        val = float(str(row[i]).replace(",", "").replace("%", "").replace("$", ""))
                        values.append(val)
                    except (ValueError, TypeError):
                        pass

            if len(values) > len(rows) * 0.5:
                total = sum(values)
                if "percent" in header.lower() or "rate" in header.lower():
                    avg = total / len(values)
                    insights.append(f"Average {header}: {avg:.1f}%")
                elif total > 1000:
                    insights.append(f"Total {header}: {total:,.0f}")

        return insights[:3]
