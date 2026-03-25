"""Auto enricher with smart selection and fallback.

Automatically selects between LLM and Basic enrichers based on
document characteristics, with fallback to BasicEnricher on failure.
"""

import time
from datetime import datetime, timezone
from typing import Any

from logging_manager import get_logger
from steps.enrich.enrichers.base import (
    Enricher,
    EnricherError,
    EnrichmentResult,
    EnrichmentInfo,
)
from steps.enrich.enrichers.basic import BasicEnricher
from steps.enrich.enrichers.llm import LLMEnricher


logger = get_logger(__name__)


# Thresholds for enricher selection
MIN_TOKENS_FOR_LLM = 500  # Documents smaller than this use BasicEnricher
COMPLEX_TABLE_THRESHOLD = 3  # 3+ tables with 5+ columns = complex
NARRATIVE_INDICATORS = [
    "paragraph", "summary", "introduction", "conclusion",
    "overview", "background", "discussion",
]


class AutoEnricher(Enricher):
    """Smart enricher that selects the best strategy automatically.

    Selection logic:
    - Document < 500 tokens → BasicEnricher (not worth LLM cost)
    - Narrative documents → LLMEnricher (benefits from summarization)
    - 3+ complex tables → LLMEnricher (benefits from descriptions)
    - Simple tabular data → BasicEnricher (rule-based sufficient)
    - LLM fails → Fallback to BasicEnricher
    """

    enricher_type = "auto"

    def __init__(self, **kwargs: Any) -> None:
        """Initialize auto enricher.

        Args:
            **kwargs: Configuration options (passed to selected enricher)
                - provider: LLM provider for LLMEnricher (default: "anthropic")
                - model: Model for LLMEnricher (default: "claude-haiku-4-5")
                - force_llm: Always use LLM enricher (default: False)
                - force_basic: Always use basic enricher (default: False)
        """
        super().__init__(**kwargs)
        self.force_llm = kwargs.get("force_llm", False)
        self.force_basic = kwargs.get("force_basic", False)

        # Remove auto-specific options before passing to child enrichers
        child_kwargs = {k: v for k, v in kwargs.items() if k not in ("force_llm", "force_basic")}
        self._llm_kwargs = child_kwargs
        self._basic_kwargs = {k: v for k, v in child_kwargs.items() if k not in ("provider", "model", "cache_enabled", "max_tokens")}

    def enrich(self, parsed_document: dict[str, Any]) -> EnrichmentResult:
        """Enrich document using automatically selected strategy.

        Args:
            parsed_document: Parsed document following parsed-document schema

        Returns:
            EnrichmentResult with enrichments from selected enricher

        Raises:
            EnricherError: If all enrichment strategies fail
        """
        start_time = time.time()

        # Select enricher strategy
        use_llm = self._should_use_llm(parsed_document)

        if self.force_basic:
            use_llm = False
        elif self.force_llm:
            use_llm = True

        logger.info(f"Auto enricher selected: {'LLM' if use_llm else 'Basic'}")

        # Try primary enricher
        if use_llm:
            try:
                enricher = LLMEnricher(**self._llm_kwargs)
                result = enricher.enrich(parsed_document)
                # Mark as auto-selected
                result.info.enricher = "auto(llm)"
                return result
            except EnricherError as e:
                logger.warning(f"LLM enricher failed, falling back to basic: {e}")
                # Fall through to basic enricher
            except Exception as e:
                logger.warning(f"LLM enricher error, falling back to basic: {e}")
                # Fall through to basic enricher

        # Use basic enricher (primary or fallback)
        try:
            enricher = BasicEnricher(**self._basic_kwargs)
            result = enricher.enrich(parsed_document)
            result.info.enricher = "auto(basic)" if not use_llm else "auto(basic-fallback)"
            return result
        except EnricherError:
            raise
        except Exception as e:
            raise EnricherError(f"All enrichment strategies failed: {e}") from e

    def _should_use_llm(self, parsed_document: dict[str, Any]) -> bool:
        """Determine if LLM enricher should be used.

        Args:
            parsed_document: Parsed document to analyze

        Returns:
            True if LLM enricher should be used
        """
        content = parsed_document.get("content", {})

        # Estimate document size
        full_text = self._get_full_text(content)
        estimated_tokens = len(full_text) // 4

        # Small documents don't benefit from LLM
        if estimated_tokens < MIN_TOKENS_FOR_LLM:
            logger.debug(f"Document too small for LLM ({estimated_tokens} tokens)")
            return False

        # Check for narrative content
        if self._has_narrative_content(content):
            logger.debug("Document has narrative content, using LLM")
            return True

        # Check for complex tables
        if self._has_complex_tables(content):
            logger.debug("Document has complex tables, using LLM")
            return True

        # Default to basic for simple tabular data
        tables = content.get("tables", [])
        sections = content.get("sections", [])

        if tables and not sections:
            logger.debug("Simple tabular data, using basic enricher")
            return False

        # Use LLM for mixed content with reasonable size
        if estimated_tokens > 1000:
            logger.debug(f"Medium-size document ({estimated_tokens} tokens), using LLM")
            return True

        return False

    def _get_full_text(self, content: dict[str, Any]) -> str:
        """Extract full text from content."""
        parts = []

        if content.get("markdown"):
            parts.append(content["markdown"])
        elif content.get("plainText"):
            parts.append(content["plainText"])

        for section in content.get("sections", []):
            if section.get("content"):
                parts.append(section["content"])

        return "\n".join(parts)

    def _has_narrative_content(self, content: dict[str, Any]) -> bool:
        """Check if document contains narrative sections."""
        sections = content.get("sections", [])

        for section in sections:
            section_type = section.get("type", "").lower()
            section_title = section.get("title", "").lower()

            # Check for narrative section types
            for indicator in NARRATIVE_INDICATORS:
                if indicator in section_type or indicator in section_title:
                    return True

            # Check for substantial text content
            section_content = section.get("content", "")
            if len(section_content) > 500:
                # Check for sentence-like structure (periods followed by capitals)
                sentences = section_content.count(". ")
                if sentences > 5:
                    return True

        return False

    def _has_complex_tables(self, content: dict[str, Any]) -> bool:
        """Check if document has complex tables worth LLM analysis."""
        tables = content.get("tables", [])

        if len(tables) < COMPLEX_TABLE_THRESHOLD:
            return False

        complex_count = 0
        for table in tables:
            headers = table.get("headers", [])
            rows = table.get("rows", [])

            # Table is complex if it has many columns or rows
            if len(headers) >= 5 or len(rows) >= 20:
                complex_count += 1

        return complex_count >= COMPLEX_TABLE_THRESHOLD
