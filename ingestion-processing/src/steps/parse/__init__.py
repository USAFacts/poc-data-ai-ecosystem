"""Document parsing step."""

from steps.parse.step import ParseStep
from steps.parse.parsers import (
    Parser,
    ParserError,
    ParseResult,
    ParsedSection,
    ParsedTable,
    ParsedKeyValue,
    VisionParser,
    BasicParser,
    register_parser,
    get_parser,
    get_registered_parsers,
    get_parser_for_format,
)
from steps.parse.quality import (
    QualityMetrics,
    ContentQuality,
    TableQuality,
    compute_quality_metrics,
)

__all__ = [
    # Step
    "ParseStep",
    # Parser base
    "Parser",
    "ParserError",
    "ParseResult",
    "ParsedSection",
    "ParsedTable",
    "ParsedKeyValue",
    # Parser implementations
    "VisionParser",
    "BasicParser",
    # Registry
    "register_parser",
    "get_parser",
    "get_registered_parsers",
    "get_parser_for_format",
    # Quality metrics
    "QualityMetrics",
    "ContentQuality",
    "TableQuality",
    "compute_quality_metrics",
]
