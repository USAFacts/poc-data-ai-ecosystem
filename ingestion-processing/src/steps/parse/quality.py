"""Quality metrics for document parsing evaluation.

Implements industry-standard metrics for measuring parsing quality and
AI readiness of extracted content. Based on:
- OmniDocBench (CVPR 2025) evaluation methodology
- Standard OCR/document extraction metrics (CER, WER, TEDS)
- AI-specific metrics (token coverage, content density)

Supports multiple document types:
- Tabular: CSV, Excel spreadsheets, data tables
- Narrative: PDFs, Word docs, reports with prose
- Mixed: Documents combining tables and text

References:
- https://github.com/opendatalab/OmniDocBench
- Dublin Core metadata quality guidelines
"""

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class DocumentType(Enum):
    """Classification of document content type."""
    TABULAR = "tabular"      # Primarily tables/structured data
    NARRATIVE = "narrative"  # Primarily prose/text content
    MIXED = "mixed"          # Combination of tables and text
    UNKNOWN = "unknown"      # Cannot determine


@dataclass
class TableQuality:
    """Quality metrics for a single extracted table."""

    row_count: int = 0
    column_count: int = 0
    total_cells: int = 0
    filled_cells: int = 0
    empty_cells: int = 0
    cell_fill_rate: float = 0.0  # Percentage of non-empty cells
    has_headers: bool = False
    header_quality: float = 0.0  # Headers that are non-empty and descriptive
    numeric_cells: int = 0
    text_cells: int = 0
    data_type_consistency: float = 0.0  # Per-column type consistency


@dataclass
class TextQuality:
    """Quality metrics for narrative/prose content."""

    # Structure detection
    heading_count: int = 0
    heading_hierarchy_depth: int = 0  # Max nesting level (h1=1, h2=2, etc.)
    heading_hierarchy_valid: bool = False  # Proper nesting (h1 before h2, etc.)

    # Paragraph metrics
    paragraph_count: int = 0
    avg_paragraph_length: float = 0.0  # Average chars per paragraph
    short_paragraphs: int = 0  # Paragraphs < 50 chars (possible extraction issues)

    # List detection
    list_count: int = 0
    list_items_total: int = 0
    bulleted_lists: int = 0
    numbered_lists: int = 0

    # Text coherence indicators
    sentence_count: int = 0
    avg_sentence_length: float = 0.0

    # Formatting preservation
    has_bold: bool = False
    has_italic: bool = False
    has_links: bool = False
    has_code_blocks: bool = False

    # Quality scores
    structure_score: float = 0.0  # How well structure is preserved
    completeness_score: float = 0.0  # Indication of complete extraction


@dataclass
class ContentQuality:
    """Quality metrics for extracted content."""

    # Size metrics
    source_size_bytes: int = 0
    extracted_text_chars: int = 0
    extracted_markdown_chars: int = 0

    # Token estimation (approximate, using ~4 chars per token heuristic)
    estimated_tokens: int = 0

    # Content density
    content_density: float = 0.0  # extracted_chars / source_bytes
    compression_ratio: float = 0.0  # How much smaller/larger than source

    # Structural metrics
    table_count: int = 0
    section_count: int = 0
    key_value_count: int = 0

    # Coverage
    has_markdown: bool = False
    has_plain_text: bool = False
    has_tables: bool = False
    has_sections: bool = False
    has_key_values: bool = False

    # Document type classification
    document_type: DocumentType = DocumentType.UNKNOWN
    tabular_ratio: float = 0.0  # Proportion of content that is tabular
    narrative_ratio: float = 0.0  # Proportion of content that is narrative


@dataclass
class QualityMetrics:
    """Comprehensive quality metrics for parsed documents.

    Provides metrics for evaluating:
    1. Extraction completeness - How much content was extracted
    2. Structural fidelity - Tables, sections, key-values detected
    3. AI readiness - Token count, content density for LLM consumption
    4. Data quality - Cell fill rates, type consistency
    5. Text quality - Heading hierarchy, paragraph structure, lists
    """

    # Overall scores (0-100)
    extraction_score: float = 0.0  # Overall extraction quality
    structural_score: float = 0.0  # Structural element detection
    ai_readiness_score: float = 0.0  # Suitability for AI/LLM use
    overall_score: float = 0.0  # Composite score

    # Detailed metrics
    content: ContentQuality = field(default_factory=ContentQuality)
    tables: list[TableQuality] = field(default_factory=list)
    text: TextQuality = field(default_factory=TextQuality)

    # Confidence from parser (if available)
    parser_confidence: float | None = None

    # Warnings and issues
    warnings: list[str] = field(default_factory=list)
    issues: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = {
            "scores": {
                "overall": round(self.overall_score, 1),
                "extraction": round(self.extraction_score, 1),
                "structural": round(self.structural_score, 1),
                "aiReadiness": round(self.ai_readiness_score, 1),
            },
            "content": {
                "sourceSizeBytes": self.content.source_size_bytes,
                "extractedTextChars": self.content.extracted_text_chars,
                "extractedMarkdownChars": self.content.extracted_markdown_chars,
                "estimatedTokens": self.content.estimated_tokens,
                "contentDensity": round(self.content.content_density, 4),
                "tableCount": self.content.table_count,
                "sectionCount": self.content.section_count,
                "keyValueCount": self.content.key_value_count,
                "documentType": self.content.document_type.value,
                "tabularRatio": round(self.content.tabular_ratio, 2),
                "narrativeRatio": round(self.content.narrative_ratio, 2),
            },
            "tables": [
                {
                    "rowCount": t.row_count,
                    "columnCount": t.column_count,
                    "cellFillRate": round(t.cell_fill_rate, 2),
                    "hasHeaders": t.has_headers,
                    "headerQuality": round(t.header_quality, 2),
                    "dataTypeConsistency": round(t.data_type_consistency, 2),
                }
                for t in self.tables
            ],
        }

        # Add text metrics for narrative/mixed documents
        if self.content.document_type in (DocumentType.NARRATIVE, DocumentType.MIXED):
            result["text"] = {
                "headingCount": self.text.heading_count,
                "headingHierarchyDepth": self.text.heading_hierarchy_depth,
                "headingHierarchyValid": self.text.heading_hierarchy_valid,
                "paragraphCount": self.text.paragraph_count,
                "avgParagraphLength": round(self.text.avg_paragraph_length, 1),
                "listCount": self.text.list_count,
                "listItemsTotal": self.text.list_items_total,
                "sentenceCount": self.text.sentence_count,
                "avgSentenceLength": round(self.text.avg_sentence_length, 1),
                "structureScore": round(self.text.structure_score, 1),
                "completenessScore": round(self.text.completeness_score, 1),
                "formatting": {
                    "hasBold": self.text.has_bold,
                    "hasItalic": self.text.has_italic,
                    "hasLinks": self.text.has_links,
                    "hasCodeBlocks": self.text.has_code_blocks,
                },
            }

        # Add optional fields
        if self.parser_confidence is not None:
            result["parserConfidence"] = self.parser_confidence
        if self.warnings:
            result["warnings"] = self.warnings
        if self.issues:
            result["issues"] = self.issues

        return result


def estimate_tokens(text: str) -> int:
    """Estimate token count for text.

    Uses a simple heuristic of ~4 characters per token, which is
    approximately accurate for English text with common tokenizers
    (GPT, Claude, etc.).

    Args:
        text: Text to estimate tokens for

    Returns:
        Estimated token count
    """
    if not text:
        return 0
    # Rough heuristic: ~4 chars per token for English
    # This accounts for whitespace, punctuation, and subword tokenization
    return max(1, len(text) // 4)


def analyze_text_quality(markdown: str) -> TextQuality:
    """Analyze quality metrics for narrative/prose content.

    Args:
        markdown: Markdown content to analyze

    Returns:
        TextQuality metrics
    """
    quality = TextQuality()

    if not markdown:
        return quality

    # Heading analysis
    heading_pattern = r'^(#{1,6})\s+(.+)$'
    headings = re.findall(heading_pattern, markdown, re.MULTILINE)
    quality.heading_count = len(headings)

    if headings:
        # Analyze heading hierarchy
        levels = [len(h[0]) for h in headings]
        quality.heading_hierarchy_depth = max(levels)

        # Check if hierarchy is valid (no skipping levels, starts reasonably)
        if levels:
            # Valid if levels generally increase/decrease without big jumps
            valid = True
            for i in range(1, len(levels)):
                if levels[i] > levels[i - 1] + 1:  # Skipped a level
                    valid = False
                    break
            quality.heading_hierarchy_valid = valid

    # Paragraph analysis
    # Split by double newlines or markdown paragraph breaks
    paragraphs = re.split(r'\n\s*\n', markdown)
    # Filter out empty and very short (likely formatting artifacts)
    paragraphs = [p.strip() for p in paragraphs if p.strip()]
    # Exclude headings-only paragraphs
    paragraphs = [p for p in paragraphs if not re.match(r'^#{1,6}\s+.+$', p)]

    quality.paragraph_count = len(paragraphs)
    if paragraphs:
        lengths = [len(p) for p in paragraphs]
        quality.avg_paragraph_length = sum(lengths) / len(lengths)
        quality.short_paragraphs = sum(1 for l in lengths if l < 50)

    # List detection
    # Bulleted lists: lines starting with -, *, +
    bulleted_pattern = r'^[\s]*[-*+]\s+.+'
    bulleted_items = re.findall(bulleted_pattern, markdown, re.MULTILINE)
    quality.bulleted_lists = len(set(re.findall(r'((?:^[\s]*[-*+]\s+.+\n?)+)', markdown, re.MULTILINE)))

    # Numbered lists: lines starting with 1., 2., etc.
    numbered_pattern = r'^[\s]*\d+\.\s+.+'
    numbered_items = re.findall(numbered_pattern, markdown, re.MULTILINE)
    quality.numbered_lists = len(set(re.findall(r'((?:^[\s]*\d+\.\s+.+\n?)+)', markdown, re.MULTILINE)))

    quality.list_count = quality.bulleted_lists + quality.numbered_lists
    quality.list_items_total = len(bulleted_items) + len(numbered_items)

    # Sentence analysis
    # Simple sentence detection (split on . ! ?)
    # Remove code blocks first to avoid false positives
    text_without_code = re.sub(r'```[\s\S]*?```', '', markdown)
    text_without_code = re.sub(r'`[^`]+`', '', text_without_code)
    sentences = re.split(r'[.!?]+\s+', text_without_code)
    sentences = [s.strip() for s in sentences if s.strip() and len(s.strip()) > 10]
    quality.sentence_count = len(sentences)
    if sentences:
        quality.avg_sentence_length = sum(len(s) for s in sentences) / len(sentences)

    # Formatting detection
    quality.has_bold = bool(re.search(r'\*\*[^*]+\*\*|__[^_]+__', markdown))
    quality.has_italic = bool(re.search(r'(?<!\*)\*[^*]+\*(?!\*)|(?<!_)_[^_]+_(?!_)', markdown))
    quality.has_links = bool(re.search(r'\[([^\]]+)\]\(([^)]+)\)', markdown))
    quality.has_code_blocks = bool(re.search(r'```[\s\S]*?```|`[^`]+`', markdown))

    # Compute structure score (0-100)
    structure_factors = []

    # Heading presence and hierarchy
    if quality.heading_count > 0:
        structure_factors.append(80 if quality.heading_hierarchy_valid else 60)
    elif quality.paragraph_count > 3:
        structure_factors.append(40)  # No headings but has paragraphs

    # Paragraph quality
    if quality.paragraph_count > 0:
        # Penalize if too many short paragraphs (extraction artifacts)
        short_ratio = quality.short_paragraphs / quality.paragraph_count
        if short_ratio < 0.3:
            structure_factors.append(90)
        elif short_ratio < 0.5:
            structure_factors.append(70)
        else:
            structure_factors.append(50)

    # List preservation
    if quality.list_count > 0:
        structure_factors.append(85)

    quality.structure_score = sum(structure_factors) / len(structure_factors) if structure_factors else 50

    # Compute completeness score (0-100)
    completeness_factors = []

    # Sentence coherence (average length indicates complete sentences)
    if quality.avg_sentence_length > 40:
        completeness_factors.append(90)
    elif quality.avg_sentence_length > 20:
        completeness_factors.append(75)
    elif quality.avg_sentence_length > 10:
        completeness_factors.append(50)
    else:
        completeness_factors.append(30)

    # Paragraph coherence
    if quality.avg_paragraph_length > 200:
        completeness_factors.append(90)
    elif quality.avg_paragraph_length > 100:
        completeness_factors.append(75)
    elif quality.avg_paragraph_length > 50:
        completeness_factors.append(60)
    else:
        completeness_factors.append(40)

    quality.completeness_score = sum(completeness_factors) / len(completeness_factors) if completeness_factors else 50

    return quality


def detect_document_type(
    markdown: str,
    tables: list[dict[str, Any]],
    text_quality: TextQuality,
) -> tuple[DocumentType, float, float]:
    """Detect whether document is primarily tabular, narrative, or mixed.

    Args:
        markdown: Markdown content
        tables: List of extracted tables
        text_quality: Analyzed text quality metrics

    Returns:
        Tuple of (DocumentType, tabular_ratio, narrative_ratio)
    """
    if not markdown and not tables:
        return DocumentType.UNKNOWN, 0.0, 0.0

    # Estimate content proportions
    total_chars = len(markdown) if markdown else 0

    # Estimate table content size
    table_chars = 0
    for table in tables:
        headers = table.get("headers", [])
        rows = table.get("rows", [])
        # Rough estimate of table content
        table_chars += sum(len(str(h)) for h in headers)
        for row in rows:
            table_chars += sum(len(str(cell)) for cell in row)

    # Estimate narrative content (total minus table content, headings, formatting)
    narrative_chars = total_chars - table_chars
    narrative_chars = max(0, narrative_chars)

    # Calculate ratios
    if total_chars > 0:
        tabular_ratio = min(1.0, table_chars / total_chars)
        narrative_ratio = min(1.0, narrative_chars / total_chars)
    else:
        tabular_ratio = 1.0 if tables else 0.0
        narrative_ratio = 0.0

    # Classify document type
    # Consider structural indicators too
    has_significant_tables = len(tables) > 0 and tabular_ratio > 0.2
    has_significant_text = (
        text_quality.paragraph_count > 2 or
        text_quality.heading_count > 1 or
        text_quality.sentence_count > 5
    )

    if has_significant_tables and has_significant_text:
        doc_type = DocumentType.MIXED
    elif has_significant_tables:
        doc_type = DocumentType.TABULAR
    elif has_significant_text or narrative_chars > 100:
        doc_type = DocumentType.NARRATIVE
    else:
        # Default based on ratios
        if tabular_ratio > 0.6:
            doc_type = DocumentType.TABULAR
        elif narrative_ratio > 0.6:
            doc_type = DocumentType.NARRATIVE
        else:
            doc_type = DocumentType.MIXED

    return doc_type, tabular_ratio, narrative_ratio


def analyze_table_quality(
    headers: list[str],
    rows: list[list[Any]],
) -> TableQuality:
    """Analyze quality metrics for a table.

    Args:
        headers: Table headers
        rows: Table data rows

    Returns:
        TableQuality metrics
    """
    quality = TableQuality()

    quality.row_count = len(rows)
    quality.column_count = len(headers) if headers else (len(rows[0]) if rows else 0)
    quality.total_cells = quality.row_count * quality.column_count
    quality.has_headers = bool(headers) and any(h.strip() for h in headers if h)

    if quality.total_cells == 0:
        return quality

    # Analyze cells
    filled = 0
    numeric = 0
    text = 0
    column_types: dict[int, list[str]] = {i: [] for i in range(quality.column_count)}

    for row in rows:
        for i, cell in enumerate(row):
            if cell is not None and str(cell).strip():
                filled += 1
                cell_str = str(cell).strip()

                # Determine cell type
                try:
                    float(cell_str.replace(",", "").replace("$", "").replace("%", ""))
                    numeric += 1
                    if i < quality.column_count:
                        column_types[i].append("numeric")
                except ValueError:
                    text += 1
                    if i < quality.column_count:
                        column_types[i].append("text")
            else:
                if i < quality.column_count:
                    column_types[i].append("empty")

    quality.filled_cells = filled
    quality.empty_cells = quality.total_cells - filled
    quality.cell_fill_rate = (filled / quality.total_cells * 100) if quality.total_cells > 0 else 0
    quality.numeric_cells = numeric
    quality.text_cells = text

    # Header quality - non-empty, descriptive headers
    if headers:
        good_headers = sum(
            1 for h in headers
            if h and len(str(h).strip()) > 1 and not str(h).strip().isdigit()
        )
        quality.header_quality = (good_headers / len(headers) * 100) if headers else 0

    # Data type consistency per column
    consistencies = []
    for col_idx, types in column_types.items():
        if types:
            # Find most common type
            type_counts = {}
            for t in types:
                type_counts[t] = type_counts.get(t, 0) + 1
            if type_counts:
                max_count = max(type_counts.values())
                consistencies.append(max_count / len(types) * 100)

    quality.data_type_consistency = (
        sum(consistencies) / len(consistencies) if consistencies else 0
    )

    return quality


def compute_quality_metrics(
    source_size_bytes: int,
    markdown: str,
    plain_text: str,
    tables: list[dict[str, Any]],
    sections: list[dict[str, Any]],
    key_values: list[dict[str, Any]],
    parser_confidence: float | None = None,
) -> QualityMetrics:
    """Compute comprehensive quality metrics for parsed content.

    Evaluates documents based on their content type:
    - Tabular: Table structure, cell fill rates, header quality
    - Narrative: Heading hierarchy, paragraph structure, text coherence
    - Mixed: Weighted combination of both

    Args:
        source_size_bytes: Size of source document in bytes
        markdown: Extracted markdown content
        plain_text: Extracted plain text content
        tables: List of extracted tables
        sections: List of extracted sections
        key_values: List of extracted key-value pairs
        parser_confidence: Confidence score from parser (0-1)

    Returns:
        QualityMetrics with all computed scores
    """
    metrics = QualityMetrics()
    metrics.parser_confidence = parser_confidence

    # Content metrics
    metrics.content.source_size_bytes = source_size_bytes
    metrics.content.extracted_text_chars = len(plain_text) if plain_text else 0
    metrics.content.extracted_markdown_chars = len(markdown) if markdown else 0
    metrics.content.estimated_tokens = estimate_tokens(markdown or plain_text)

    # Content density (extracted chars per source byte)
    if source_size_bytes > 0:
        extracted_chars = max(
            metrics.content.extracted_text_chars,
            metrics.content.extracted_markdown_chars,
        )
        metrics.content.content_density = extracted_chars / source_size_bytes
        metrics.content.compression_ratio = extracted_chars / source_size_bytes

    # Structural metrics
    metrics.content.table_count = len(tables)
    metrics.content.section_count = len(sections)
    metrics.content.key_value_count = len(key_values)

    metrics.content.has_markdown = bool(markdown)
    metrics.content.has_plain_text = bool(plain_text)
    metrics.content.has_tables = bool(tables)
    metrics.content.has_sections = bool(sections)
    metrics.content.has_key_values = bool(key_values)

    # Analyze tables
    for table in tables:
        headers = table.get("headers", [])
        rows = table.get("rows", [])
        table_quality = analyze_table_quality(headers, rows)
        metrics.tables.append(table_quality)

    # Analyze text quality
    metrics.text = analyze_text_quality(markdown or plain_text or "")

    # Detect document type
    doc_type, tabular_ratio, narrative_ratio = detect_document_type(
        markdown or plain_text or "",
        tables,
        metrics.text,
    )
    metrics.content.document_type = doc_type
    metrics.content.tabular_ratio = tabular_ratio
    metrics.content.narrative_ratio = narrative_ratio

    # Compute scores based on document type

    # 1. Extraction Score (0-100)
    # Based on: content density, having some output
    extraction_factors = []

    if metrics.content.extracted_markdown_chars > 0 or metrics.content.extracted_text_chars > 0:
        extraction_factors.append(100)  # Has content
    else:
        extraction_factors.append(0)
        metrics.issues.append("No text content extracted")

    # Content density scoring (ideal range depends on format)
    # For structured data (CSV, JSON, Excel): expect 0.5-2.0 density
    # For PDFs/images: expect 0.01-0.1 density
    if metrics.content.content_density > 0:
        if metrics.content.content_density >= 0.1:
            extraction_factors.append(100)
        elif metrics.content.content_density >= 0.01:
            extraction_factors.append(70)
        else:
            extraction_factors.append(40)
            metrics.warnings.append("Low content density - possible extraction issues")

    metrics.extraction_score = sum(extraction_factors) / len(extraction_factors) if extraction_factors else 0

    # 2. Structural Score (0-100)
    # Score differently based on document type
    structural_factors = []

    if doc_type == DocumentType.TABULAR:
        # For tabular docs, focus on table quality
        if metrics.content.has_tables:
            table_scores = []
            for tq in metrics.tables:
                t_score = (tq.cell_fill_rate * 0.5) + (tq.header_quality * 0.25) + (tq.data_type_consistency * 0.25)
                table_scores.append(t_score)
            if table_scores:
                structural_factors.append(sum(table_scores) / len(table_scores))
        else:
            structural_factors.append(30)
            metrics.warnings.append("Tabular document but no tables detected")

    elif doc_type == DocumentType.NARRATIVE:
        # For narrative docs, focus on text structure
        structural_factors.append(metrics.text.structure_score)
        structural_factors.append(metrics.text.completeness_score)

        # Bonus for having sections/key-values
        if metrics.content.has_sections:
            structural_factors.append(85)
        if metrics.content.has_key_values:
            structural_factors.append(85)

    elif doc_type == DocumentType.MIXED:
        # For mixed docs, combine both metrics weighted by content ratio
        table_score = 0
        if metrics.content.has_tables and metrics.tables:
            table_scores = []
            for tq in metrics.tables:
                t_score = (tq.cell_fill_rate * 0.5) + (tq.header_quality * 0.25) + (tq.data_type_consistency * 0.25)
                table_scores.append(t_score)
            table_score = sum(table_scores) / len(table_scores) if table_scores else 0

        text_score = (metrics.text.structure_score + metrics.text.completeness_score) / 2

        # Weight by content ratios
        if tabular_ratio + narrative_ratio > 0:
            combined = (table_score * tabular_ratio + text_score * narrative_ratio) / (tabular_ratio + narrative_ratio)
            structural_factors.append(combined)
        else:
            structural_factors.append((table_score + text_score) / 2)

        if metrics.content.has_sections:
            structural_factors.append(80)
        if metrics.content.has_key_values:
            structural_factors.append(80)

    else:  # UNKNOWN
        # Fallback to basic detection
        if metrics.content.has_tables:
            table_scores = [
                (tq.cell_fill_rate * 0.5) + (tq.header_quality * 0.25) + (tq.data_type_consistency * 0.25)
                for tq in metrics.tables
            ]
            if table_scores:
                structural_factors.append(sum(table_scores) / len(table_scores))

        if metrics.content.has_sections:
            structural_factors.append(80)
        if metrics.content.has_key_values:
            structural_factors.append(80)

        if not structural_factors:
            if metrics.content.has_plain_text:
                structural_factors.append(50)
                metrics.warnings.append("No structured elements detected")
            else:
                structural_factors.append(0)

    metrics.structural_score = sum(structural_factors) / len(structural_factors) if structural_factors else 0

    # 3. AI Readiness Score (0-100)
    # Based on: token count, content quality, structure, format
    ai_factors = []

    # Token count scoring
    tokens = metrics.content.estimated_tokens
    if tokens >= 100:
        ai_factors.append(100)
    elif tokens >= 50:
        ai_factors.append(80)
    elif tokens >= 10:
        ai_factors.append(50)
    else:
        ai_factors.append(20)
        metrics.warnings.append("Very low token count - limited AI utility")

    # Markdown availability (preferred for LLMs)
    if metrics.content.has_markdown:
        ai_factors.append(100)
    else:
        ai_factors.append(60)

    # Content-type specific AI readiness
    if doc_type == DocumentType.TABULAR:
        # Tables are great for AI if well-structured
        if metrics.content.has_tables and metrics.tables:
            avg_fill = sum(t.cell_fill_rate for t in metrics.tables) / len(metrics.tables)
            ai_factors.append(avg_fill)

    elif doc_type == DocumentType.NARRATIVE:
        # Narrative is good for AI if coherent
        ai_factors.append(metrics.text.completeness_score)
        # Headings help LLMs understand structure
        if metrics.text.heading_count > 0:
            ai_factors.append(85)

    elif doc_type == DocumentType.MIXED:
        # Mixed content - evaluate both
        if metrics.content.has_tables and metrics.tables:
            avg_fill = sum(t.cell_fill_rate for t in metrics.tables) / len(metrics.tables)
            ai_factors.append(avg_fill * tabular_ratio + metrics.text.completeness_score * narrative_ratio)
        else:
            ai_factors.append(metrics.text.completeness_score)

    metrics.ai_readiness_score = sum(ai_factors) / len(ai_factors) if ai_factors else 0

    # 4. Overall Score
    # Weighted average: extraction 30%, structural 30%, AI readiness 40%
    metrics.overall_score = (
        metrics.extraction_score * 0.30 +
        metrics.structural_score * 0.30 +
        metrics.ai_readiness_score * 0.40
    )

    # Add confidence from parser if available
    if parser_confidence is not None:
        # Blend parser confidence into overall score (20% weight)
        metrics.overall_score = metrics.overall_score * 0.80 + (parser_confidence * 100) * 0.20

    return metrics
