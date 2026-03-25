"""Document parsers for structured content extraction.

Parsers are pluggable components that extract structured content from documents.
"""

# Route MuPDF messages through Python logging instead of raw stderr.
# pymupdf 1.27+ respects PYMUPDF_MESSAGE env var.
# Using "logging:" sends messages to Python's logging module where we can
# filter out known-noisy warnings while still capturing real errors.
import logging as _logging
import os as _os

if "PYMUPDF_MESSAGE" not in _os.environ:
    _os.environ["PYMUPDF_MESSAGE"] = "logging:name=pipeline.mupdf,level=20"


class _MuPDFNoiseFilter(_logging.Filter):
    """Filter out known-noisy MuPDF messages while keeping real errors."""

    NOISE_PATTERNS = (
        "No common ancestor",
        "format error: No common ancestor in structure tree",
    )

    def filter(self, record: _logging.LogRecord) -> bool:
        msg = record.getMessage()
        return not any(pattern in msg for pattern in self.NOISE_PATTERNS)


_mupdf_logger = _logging.getLogger("pipeline.mupdf")
_mupdf_logger.addFilter(_MuPDFNoiseFilter())

from steps.parse.parsers.base import (
    Parser,
    ParserError,
    ParseResult,
    ParsedSection,
    ParsedTable,
    ParsedKeyValue,
)
from steps.parse.parsers.registry import (
    register_parser,
    get_parser,
    get_parser_class,
    get_registered_parsers,
    get_parser_for_format,
)
from steps.parse.parsers.basic import BasicParser
from steps.parse.parsers.vision import VisionParser

__all__ = [
    # Base classes
    "Parser",
    "ParserError",
    "ParseResult",
    "ParsedSection",
    "ParsedTable",
    "ParsedKeyValue",
    # Registry
    "register_parser",
    "get_parser",
    "get_parser_class",
    "get_registered_parsers",
    "get_parser_for_format",
    # Implementations
    "BasicParser",
    "VisionParser",
]
