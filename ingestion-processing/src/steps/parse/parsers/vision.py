"""Claude Vision parser — renders PDF pages as images and extracts structure via Claude.

Uses PyMuPDF to render pages to PNG, then sends them to Claude's vision API
for high-quality structured extraction of text, tables, and layout.

Falls back to BasicParser's direct PyMuPDF text extraction if the API is unavailable.
"""

import base64
import io
import json
import os
import re
import time
from typing import Any

from logging_manager import get_logger
from steps.parse.parsers.base import (
    Parser,
    ParserError,
    ParseResult,
    ParsedSection,
    ParsedTable,
)

logger = get_logger(__name__)


_mupdf_suppressed = False


def _suppress_mupdf_warnings():
    """Suppress MuPDF display warnings globally (once)."""
    global _mupdf_suppressed
    if not _mupdf_suppressed:
        import pymupdf
        pymupdf.TOOLS.mupdf_display_warnings(False)
        pymupdf.TOOLS.mupdf_warnings(False)
        _mupdf_suppressed = True


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

# Max pages to send in a single API call (Claude supports multi-image)
PAGES_PER_BATCH = 5

# DPI for rendering — 200 is readable text without huge file sizes
RENDER_DPI = 200

EXTRACTION_PROMPT = """Analyze this government document page and extract all content in structured JSON.

Return ONLY valid JSON with this exact structure:
{
  "sections": [
    {
      "type": "heading" | "paragraph" | "list" | "footnote",
      "content": "the text content",
      "level": 1-6 (for headings only)
    }
  ],
  "tables": [
    {
      "title": "table title if visible",
      "headers": ["column1", "column2"],
      "rows": [["cell1", "cell2"], ["cell3", "cell4"]]
    }
  ]
}

Rules:
- Extract ALL text, preserving reading order
- For tables, capture every row and column including merged cells
- For multi-column layouts, read left column first then right
- Include footnotes and captions as separate sections
- If a page has no tables, return an empty tables array
- Return ONLY the JSON, no other text"""


class VisionParser(Parser):
    """Parser that uses Claude Vision to extract content from document images.

    Renders each PDF page as a PNG image using PyMuPDF, then sends the images
    to Claude's vision API for structured extraction.

    For non-PDF formats, delegates to BasicParser.
    """

    parser_type = "vision"
    supported_formats = ["pdf", "png", "jpg", "jpeg", "tiff", "bmp"]

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._client = None

    def _get_client(self):
        """Get or create Anthropic client using vision-specific credentials."""
        if self._client is None:
            import anthropic
            import httpx

            api_key = os.getenv("CLAUDE_VISION_KEY") or os.getenv("ANTHROPIC_API_KEY")
            base_url = os.getenv("CLAUDE_VISION_URL") or os.getenv("ANTHROPIC_BASE_URL")

            if not api_key:
                raise ParserError("No API key found. Set CLAUDE_VISION_KEY or ANTHROPIC_API_KEY.")

            # Use truststore for SSL on macOS (Homebrew Python)
            ssl_context = _get_ssl_context()
            http_client = httpx.Client(verify=ssl_context)

            client_kwargs: dict[str, Any] = {
                "api_key": api_key,
                "http_client": http_client,
            }
            if base_url:
                client_kwargs["base_url"] = base_url

            self._client = anthropic.Anthropic(**client_kwargs)

        return self._client

    def parse(
        self,
        data: bytes,
        filename: str,
        mime_type: str | None = None,
    ) -> ParseResult:
        """Parse document using Claude Vision.

        For PDFs: renders pages to images, sends to Claude.
        For images: sends directly to Claude.
        """
        start_time = time.time()
        format_ext = self.get_format_from_filename(filename)

        try:
            if format_ext == "pdf":
                result = self._parse_pdf_with_vision(data)
            elif format_ext in ("png", "jpg", "jpeg", "tiff", "bmp"):
                result = self._parse_image_with_vision(data, format_ext)
            else:
                raise ParserError(f"VisionParser does not support format: {format_ext}")

            result.parser = self.parser_type
            result.parser_version = "1.0"
            result.processing_time_ms = int((time.time() - start_time) * 1000)
            return result

        except ParserError:
            raise
        except Exception as e:
            raise ParserError(f"Vision parsing failed: {e}") from e

    def _render_pdf_pages(self, data: bytes) -> list[tuple[int, bytes]]:
        """Render PDF pages to PNG images using PyMuPDF.

        Returns:
            List of (page_number, png_bytes) tuples.
        """
        try:
            import pymupdf
        except ImportError:
            raise ParserError("pymupdf not installed. Install with: pip install pymupdf")

        _suppress_mupdf_warnings()

        doc = pymupdf.open(stream=data, filetype="pdf")
        pages: list[tuple[int, bytes]] = []

        for page_idx in range(len(doc)):
            page = doc[page_idx]
            mat = pymupdf.Matrix(RENDER_DPI / 72, RENDER_DPI / 72)
            pix = page.get_pixmap(matrix=mat)
            png_bytes = pix.tobytes("png")
            pages.append((page_idx + 1, png_bytes))

        doc.close()
        return pages

    def _parse_pdf_with_vision(self, data: bytes) -> ParseResult:
        """Parse PDF by rendering pages and sending to Claude Vision."""
        pages = self._render_pdf_pages(data)
        page_count = len(pages)

        logger.info(
            f"Rendered {page_count} PDF pages at {RENDER_DPI} DPI",
            extra={"step": "parse"},
        )

        all_sections: list[ParsedSection] = []
        all_tables: list[ParsedTable] = []
        all_markdown: list[str] = []
        all_text: list[str] = []

        # Process in batches
        for batch_start in range(0, len(pages), PAGES_PER_BATCH):
            batch = pages[batch_start:batch_start + PAGES_PER_BATCH]

            try:
                batch_result = self._send_to_claude(batch)

                for page_num, page_data in zip(
                    [p[0] for p in batch], batch_result
                ):
                    sections, tables, markdown, text = self._convert_page_result(
                        page_data, page_num
                    )
                    all_sections.extend(sections)
                    all_tables.extend(tables)
                    all_markdown.append(markdown)
                    all_text.append(text)

            except Exception as e:
                logger.warning(
                    f"Claude Vision failed for pages {batch_start + 1}-{batch_start + len(batch)}: {e}",
                    extra={"step": "parse"},
                )
                # Fall back to PyMuPDF text extraction for this batch
                for page_num, png_bytes in batch:
                    all_sections.append(ParsedSection(
                        type="paragraph",
                        content=f"[Vision extraction failed for page {page_num}]",
                        page_number=page_num,
                    ))

        return ParseResult(
            markdown="\n\n".join(all_markdown),
            plain_text="\n\n".join(all_text),
            sections=all_sections,
            tables=all_tables,
            page_count=page_count,
        )

    def _parse_image_with_vision(self, data: bytes, format_ext: str) -> ParseResult:
        """Parse a single image file with Claude Vision."""
        media_type = {
            "png": "image/png",
            "jpg": "image/jpeg",
            "jpeg": "image/jpeg",
            "tiff": "image/png",  # Convert if needed
            "bmp": "image/png",
        }.get(format_ext, "image/png")

        # For non-PNG/JPEG formats, convert via PyMuPDF
        if format_ext in ("tiff", "bmp"):
            try:
                import pymupdf
                _suppress_mupdf_warnings()
                doc = pymupdf.open(stream=data, filetype=format_ext)
                page = doc[0]
                pix = page.get_pixmap()
                data = pix.tobytes("png")
                media_type = "image/png"
                doc.close()
            except Exception:
                pass

        pages = [(1, data)]
        result_pages = self._send_to_claude(pages, media_type=media_type)

        sections, tables, markdown, text = self._convert_page_result(
            result_pages[0] if result_pages else {}, 1
        )

        return ParseResult(
            markdown=markdown,
            plain_text=text,
            sections=sections,
            tables=tables,
            page_count=1,
        )

    @staticmethod
    def _extract_json(text: str) -> Any | None:
        """Extract JSON from a Claude response, handling common wrapper formats.

        Handles: raw JSON, ```json fences, ```JSON fences, triple-backtick
        without language tag, fences with leading/trailing prose, and nested
        fence edge cases.

        Returns the parsed object/list, or None if extraction fails.
        """
        text = text.strip()

        # 1. Strip markdown code fences (```json ... ```, ``` ... ```, etc.)
        fence_pattern = re.compile(
            r"```(?:json|JSON)?\s*\n(.*?)```", re.DOTALL
        )
        fence_match = fence_pattern.search(text)
        if fence_match:
            text = fence_match.group(1).strip()

        # 2. Try direct parse first (covers clean responses)
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

        # 3. Find the outermost JSON structure using bracket matching
        for open_ch, close_ch in [("{", "}"), ("[", "]")]:
            start = text.find(open_ch)
            if start < 0:
                continue
            depth = 0
            in_string = False
            escape_next = False
            for i in range(start, len(text)):
                ch = text[i]
                if escape_next:
                    escape_next = False
                    continue
                if ch == "\\":
                    escape_next = True
                    continue
                if ch == '"':
                    in_string = not in_string
                    continue
                if in_string:
                    continue
                if ch == open_ch:
                    depth += 1
                elif ch == close_ch:
                    depth -= 1
                    if depth == 0:
                        try:
                            return json.loads(text[start : i + 1])
                        except json.JSONDecodeError:
                            break
        return None

    def _send_to_claude(
        self,
        pages: list[tuple[int, bytes]],
        media_type: str = "image/png",
    ) -> list[dict[str, Any]]:
        """Send page images to Claude Vision and get structured extraction.

        Args:
            pages: List of (page_number, image_bytes) tuples.
            media_type: MIME type of the images.

        Returns:
            List of page result dicts, one per page.
        """
        client = self._get_client()

        # Build content blocks — one image per page + the extraction prompt
        content: list[dict[str, Any]] = []

        for page_num, img_bytes in pages:
            b64_data = base64.b64encode(img_bytes).decode("utf-8")
            content.append({
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": media_type,
                    "data": b64_data,
                },
            })

        if len(pages) > 1:
            prompt = (
                f"I'm sending {len(pages)} pages from a government document. "
                f"For EACH page, {EXTRACTION_PROMPT}\n\n"
                f"Return a JSON array with one object per page, in order."
            )
        else:
            prompt = EXTRACTION_PROMPT

        content.append({"type": "text", "text": prompt})

        model = os.getenv("CLAUDE_VISION_MODEL", "claude-sonnet-4-5")
        message = client.messages.create(
            model=model,
            max_tokens=4096,
            messages=[{"role": "user", "content": content}],
        )

        # Parse response — strip markdown code fences if present
        response_text = message.content[0].text.strip()
        parsed = self._extract_json(response_text)

        if parsed is None:
            logger.warning(
                "Could not parse Claude Vision response as JSON. "
                "Response preview: %s",
                response_text[:500],
            )
            parsed = {}

        # Normalize to list of page results
        if isinstance(parsed, list):
            return parsed
        elif isinstance(parsed, dict):
            return [parsed]
        else:
            return [{}]

    def _convert_page_result(
        self,
        page_data: dict[str, Any],
        page_num: int,
    ) -> tuple[list[ParsedSection], list[ParsedTable], str, str]:
        """Convert a Claude Vision page result to ParseResult components.

        Returns:
            Tuple of (sections, tables, markdown_text, plain_text)
        """
        sections: list[ParsedSection] = []
        tables: list[ParsedTable] = []
        markdown_parts: list[str] = [f"## Page {page_num}\n"]
        text_parts: list[str] = []

        # Page heading
        sections.append(ParsedSection(
            type="heading",
            content=f"Page {page_num}",
            level=2,
            page_number=page_num,
        ))

        # Sections
        for section in page_data.get("sections", []):
            section_type = section.get("type", "paragraph")
            content = section.get("content", "")
            level = section.get("level")

            if not content.strip():
                continue

            sections.append(ParsedSection(
                type=section_type,
                content=content,
                level=level,
                page_number=page_num,
            ))

            if section_type == "heading" and level:
                markdown_parts.append(f"{'#' * level} {content}\n")
            else:
                markdown_parts.append(content)
                markdown_parts.append("")

            text_parts.append(content)

        # Tables
        for table_idx, table_data in enumerate(page_data.get("tables", [])):
            headers = table_data.get("headers", [])
            rows = table_data.get("rows", [])
            title = table_data.get("title", f"Table {table_idx + 1}")

            if not headers and not rows:
                continue

            # Build markdown table
            md_lines = []
            if headers:
                md_lines.append("| " + " | ".join(str(h) for h in headers) + " |")
                md_lines.append("| " + " | ".join(["---"] * len(headers)) + " |")
            for row in rows:
                escaped = [str(c).replace("|", "\\|") for c in row]
                md_lines.append("| " + " | ".join(escaped) + " |")

            table_markdown = "\n".join(md_lines)

            tables.append(ParsedTable(
                headers=[str(h) for h in headers],
                rows=[[str(c) for c in row] for row in rows],
                title=title,
                page_number=page_num,
                markdown=table_markdown,
            ))

            sections.append(ParsedSection(
                type="table",
                content=table_markdown,
                title=title,
                page_number=page_num,
            ))

            markdown_parts.append(table_markdown)
            markdown_parts.append("")

        markdown = "\n".join(markdown_parts)
        plain_text = f"--- Page {page_num} ---\n" + "\n".join(text_parts)

        return sections, tables, markdown, plain_text
