"""Base parser interface for document parsing."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from steps.parse.quality import QualityMetrics


class ParserError(Exception):
    """Error during parser operation."""

    pass


@dataclass
class ParsedSection:
    """A section extracted from a document.

    For PDFs: page_number is the page (1-based)
    For Excel: page_number is the sheet index (1-based), sheet_name is the sheet name
    """

    type: str  # header, footer, title, paragraph, table, etc.
    content: str
    level: int | None = None
    title: str | None = None
    page_number: int | None = None  # Page number (PDF) or sheet index (Excel)
    sheet_name: str | None = None  # Sheet name for Excel files
    confidence: float | None = None
    bounding_box: dict[str, float] | None = None


@dataclass
class ParsedTable:
    """A table extracted from a document.

    For PDFs: page_number is the page (1-based)
    For Excel: page_number is the sheet index (1-based), sheet_name is the sheet name
    """

    headers: list[str]
    rows: list[list[Any]]
    title: str | None = None
    page_number: int | None = None  # Page number (PDF) or sheet index (Excel)
    sheet_name: str | None = None  # Sheet name for Excel files
    markdown: str | None = None
    confidence: float | None = None


@dataclass
class ParsedKeyValue:
    """A key-value pair extracted from a document."""

    key: str
    value: Any
    value_type: str = "string"  # string, number, date, currency, etc.
    confidence: float | None = None


@dataclass
class ParseResult:
    """Result from a parser operation.

    Contains structured content following the parsed document schema.
    """

    # Core content
    markdown: str = ""
    plain_text: str = ""

    # Structured elements
    sections: list[ParsedSection] = field(default_factory=list)
    tables: list[ParsedTable] = field(default_factory=list)
    key_value_pairs: list[ParsedKeyValue] = field(default_factory=list)

    # Metadata
    page_count: int | None = None
    title: str | None = None
    language: str | None = None

    # Extraction info
    parser: str = ""
    parser_version: str = ""
    model: str | None = None
    processing_time_ms: int | None = None
    confidence: float | None = None
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def to_content_dict(self) -> dict[str, Any]:
        """Convert to content section of parsed document schema."""
        content: dict[str, Any] = {}

        if self.markdown:
            content["markdown"] = self.markdown
        if self.plain_text:
            content["plainText"] = self.plain_text

        if self.sections:
            content["sections"] = [
                {
                    "type": s.type,
                    "content": s.content,
                    **({"level": s.level} if s.level else {}),
                    **({"title": s.title} if s.title else {}),
                    **({"pageNumber": s.page_number} if s.page_number else {}),
                    **({"sheetName": s.sheet_name} if s.sheet_name else {}),
                    **({"confidence": s.confidence} if s.confidence else {}),
                }
                for s in self.sections
            ]

        if self.tables:
            content["tables"] = [
                {
                    "headers": t.headers,
                    "rows": t.rows,
                    "rowCount": len(t.rows),
                    "columnCount": len(t.headers) if t.headers else (len(t.rows[0]) if t.rows else 0),
                    **({"title": t.title} if t.title else {}),
                    **({"pageNumber": t.page_number} if t.page_number else {}),
                    **({"sheetName": t.sheet_name} if t.sheet_name else {}),
                    **({"markdown": t.markdown} if t.markdown else {}),
                    **({"confidence": t.confidence} if t.confidence else {}),
                }
                for t in self.tables
            ]

        if self.key_value_pairs:
            content["keyValuePairs"] = [
                {
                    "key": kv.key,
                    "value": kv.value,
                    "valueType": kv.value_type,
                    **({"confidence": kv.confidence} if kv.confidence else {}),
                }
                for kv in self.key_value_pairs
            ]

        return content

    def to_extraction_dict(self) -> dict[str, Any]:
        """Convert to extraction section of parsed document schema."""
        extraction: dict[str, Any] = {
            "parser": self.parser,
            "extractedAt": datetime.now(timezone.utc).isoformat(),
        }

        if self.parser_version:
            extraction["parserVersion"] = self.parser_version
        if self.model:
            extraction["model"] = self.model
        if self.processing_time_ms is not None:
            extraction["processingTimeMs"] = self.processing_time_ms
        if self.confidence is not None:
            extraction["confidence"] = self.confidence
        if self.warnings:
            extraction["warnings"] = self.warnings
        if self.errors:
            extraction["errors"] = self.errors

        return extraction

    def compute_quality(self, source_size_bytes: int) -> "QualityMetrics":
        """Compute quality metrics for the parsed content.

        Args:
            source_size_bytes: Size of source document in bytes

        Returns:
            QualityMetrics with extraction quality scores
        """
        from steps.parse.quality import compute_quality_metrics

        tables_data = [
            {"headers": t.headers, "rows": t.rows}
            for t in self.tables
        ]
        sections_data = [
            {"type": s.type, "content": s.content}
            for s in self.sections
        ]
        key_values_data = [
            {"key": kv.key, "value": kv.value}
            for kv in self.key_value_pairs
        ]

        return compute_quality_metrics(
            source_size_bytes=source_size_bytes,
            markdown=self.markdown,
            plain_text=self.plain_text,
            tables=tables_data,
            sections=sections_data,
            key_values=key_values_data,
            parser_confidence=self.confidence,
        )


class Parser(ABC):
    """Abstract base class for document parsers.

    Parsers are responsible for extracting structured content from documents.
    Each parser type handles specific file formats or uses specific services
    (Nanonets, Azure, Textract, etc.).

    Subclasses must implement:
    - parser_type: Class attribute identifying the parser type
    - parse(): Method to extract content from document
    - supported_formats: List of supported file formats
    """

    # Parser type identifier (override in subclasses)
    parser_type: str = "base"

    # Supported file formats (override in subclasses)
    supported_formats: list[str] = []

    def __init__(self, **kwargs: Any) -> None:
        """Initialize parser with optional configuration."""
        self.config = kwargs

    @abstractmethod
    def parse(
        self,
        data: bytes,
        filename: str,
        mime_type: str | None = None,
    ) -> ParseResult:
        """Parse a document and extract structured content.

        Args:
            data: Raw document bytes
            filename: Original filename (used for format detection)
            mime_type: MIME type if known

        Returns:
            ParseResult with extracted content

        Raises:
            ParserError: If parsing fails
        """
        pass

    def supports_format(self, format: str) -> bool:
        """Check if parser supports a file format.

        Args:
            format: File extension (without dot)

        Returns:
            True if format is supported
        """
        return format.lower() in [f.lower() for f in self.supported_formats]

    def get_format_from_filename(self, filename: str) -> str:
        """Extract format from filename.

        Args:
            filename: Filename with extension

        Returns:
            File extension without dot
        """
        if "." in filename:
            return filename.rsplit(".", 1)[-1].lower()
        return ""

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(type={self.parser_type!r})"
