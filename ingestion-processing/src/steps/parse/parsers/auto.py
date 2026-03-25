"""Auto parser with intelligent fallback logic."""

from typing import Any

from logging_manager import get_logger
from steps.parse.parsers.base import (
    Parser,
    ParserError,
    ParseResult,
    ParsedKeyValue,
)

logger = get_logger(__name__)


# Formats that use Claude Vision (PDF, images, complex documents)
VISION_FORMATS = {"pdf", "png", "jpg", "jpeg", "tiff", "bmp"}

# Formats that go directly to BasicParser (structured data)
BASIC_ONLY_FORMATS = {"csv", "json", "txt", "xml", "xlsx", "xls"}


class AutoParser(Parser):
    """Intelligent parser that auto-selects the best parser for each format.

    Selection logic:
    - CSV, JSON, text, XML, Excel: BasicParser directly (structured data)
    - PDF, images: Claude Vision first, falls back to BasicParser (PyMuPDF)

    Fallback behavior:
    - If Claude Vision fails (API error, no key), falls back to BasicParser
      which uses PyMuPDF for direct text/table extraction
    """

    parser_type = "auto"
    supported_formats = list(VISION_FORMATS | BASIC_ONLY_FORMATS)

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._basic_parser = None
        self._vision_parser = None
        self._kwargs = kwargs

    def _get_basic_parser(self) -> Parser:
        """Get or create BasicParser instance."""
        if self._basic_parser is None:
            from steps.parse.parsers.basic import BasicParser
            self._basic_parser = BasicParser(**self._kwargs)
        return self._basic_parser

    def _get_vision_parser(self) -> "Parser | None":
        """Get or create VisionParser instance.

        Returns None if Claude Vision is not configured.
        """
        if self._vision_parser is None:
            try:
                import os
                # Check if vision credentials are available
                has_key = bool(
                    os.getenv("CLAUDE_VISION_KEY") or os.getenv("ANTHROPIC_API_KEY")
                )
                if not has_key:
                    logger.debug("No Claude Vision API key configured")
                    return None

                from steps.parse.parsers.vision import VisionParser
                self._vision_parser = VisionParser(**self._kwargs)
            except Exception as e:
                logger.warning(f"Vision parser not available: {e}")
                return None
        return self._vision_parser

    def parse(
        self,
        data: bytes,
        filename: str,
        mime_type: str | None = None,
    ) -> ParseResult:
        """Parse document using the best available parser.

        For PDFs and images, tries Claude Vision first and falls back
        to BasicParser (PyMuPDF) on failure.
        """
        format_ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

        # Structured formats — BasicParser directly
        if format_ext in BASIC_ONLY_FORMATS:
            logger.debug(f"Using BasicParser for {format_ext} format")
            return self._get_basic_parser().parse(data, filename, mime_type)

        # PDF and images — try Claude Vision with BasicParser fallback
        if format_ext in VISION_FORMATS:
            vision = self._get_vision_parser()

            if vision is not None:
                try:
                    logger.info(f"Using Claude Vision for {format_ext}: {filename}")
                    result = vision.parse(data, filename, mime_type)

                    if self._is_valid_result(result):
                        return result
                    else:
                        logger.warning(
                            f"Claude Vision returned empty result for {filename}, "
                            "falling back to BasicParser"
                        )
                except ParserError as e:
                    logger.warning(
                        f"Claude Vision failed for {filename}: {e}, "
                        "falling back to BasicParser"
                    )
                except Exception as e:
                    logger.warning(
                        f"Unexpected Vision error for {filename}: {e}, "
                        "falling back to BasicParser"
                    )

            # Fall back to BasicParser (PyMuPDF direct extraction)
            logger.info(f"Using BasicParser (PyMuPDF) fallback for {filename}")
            try:
                return self._get_basic_parser().parse(data, filename, mime_type)
            except ParserError as e:
                logger.warning(
                    f"BasicParser fallback also failed for {filename}: {e}. "
                    "Returning empty result."
                )
                return self._create_empty_result(
                    filename=filename,
                    primary_error="Claude Vision unavailable or failed",
                    fallback_error=str(e),
                )

        # Unknown format — try BasicParser
        logger.debug(f"Unknown format '{format_ext}', trying BasicParser")
        return self._get_basic_parser().parse(data, filename, mime_type)

    def _is_valid_result(self, result: ParseResult) -> bool:
        """Check if a parse result contains meaningful content."""
        has_markdown = bool(result.markdown and result.markdown.strip())
        has_text = bool(result.plain_text and result.plain_text.strip())
        has_tables = bool(result.tables)
        has_sections = bool(result.sections)
        has_key_values = bool(result.key_value_pairs)

        for kv in result.key_value_pairs:
            if kv.key == "error":
                return False

        return has_markdown or has_text or has_tables or has_sections or has_key_values

    def _create_empty_result(
        self,
        filename: str,
        primary_error: str,
        fallback_error: str,
    ) -> ParseResult:
        """Create an empty result with warnings when all parsers fail."""
        return ParseResult(
            markdown="",
            plain_text="",
            sections=[],
            tables=[],
            key_value_pairs=[
                ParsedKeyValue(key="parsing_status", value="failed", value_type="string"),
                ParsedKeyValue(key="primary_parser_error", value=primary_error, value_type="string"),
                ParsedKeyValue(key="fallback_parser_error", value=fallback_error, value_type="string"),
            ],
            parser="auto",
            parser_version="fallback-empty",
            warnings=[
                f"Primary parser failed: {primary_error}",
                f"Fallback parser failed: {fallback_error}",
                "No content could be extracted from this document",
            ],
            errors=[f"All parsing attempts failed for {filename}"],
        )
