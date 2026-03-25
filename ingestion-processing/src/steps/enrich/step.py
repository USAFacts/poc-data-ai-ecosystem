"""Enrichment step implementation."""

import json
from datetime import datetime, timezone
from typing import Any

from logging_manager import get_logger
from runtime.context import ExecutionContext
from steps.base import Step, StepResult, StepStatus
from steps.enrich.enrichers import (
    Enricher,
    EnricherError,
    get_enricher,
)
from storage.naming import PARSED_ZONE, ENRICHMENT_ZONE

logger = get_logger(__name__)

# Schema identifier for enriched documents
ENRICHED_DOCUMENT_SCHEMA = "https://usafacts.org/schemas/enriched-document/v1"


class EnrichmentStep(Step):
    """Step that enriches parsed documents with semantic context for RAG.

    Reads documents from parsed-zone, enriches them with summaries,
    entities, and structured metadata, and stores results in enrichment-zone.

    The output follows the enriched document schema which extends the
    parsed document schema with an enrichment section containing:
    - Document-level: summary, key topics, entities, temporal scope
    - Section-level: summaries, key points, relevant queries
    - Table-level: descriptions, column descriptions, insights

    Configuration options:
    - enricher: Enricher type to use (auto, llm, basic)
    - enricher_config: Enricher-specific configuration
    - skip_if_exists: Skip enrichment if output already exists
    """

    step_type = "enrichment"

    def __init__(self, name: str, config: dict[str, Any] | None = None) -> None:
        """Initialize enrichment step.

        Args:
            name: Step name
            config: Step configuration
        """
        super().__init__(name, config)
        self._enricher_cache: dict[str, Enricher] = {}

    def _get_enricher(self, enricher_type: str) -> Enricher:
        """Get or create an enricher instance.

        Args:
            enricher_type: Requested enricher type

        Returns:
            Enricher instance

        Raises:
            ValueError: If no suitable enricher found
        """
        enricher_config = self.config.get("enricher_config", {})

        if enricher_type not in self._enricher_cache:
            enricher = get_enricher(enricher_type, **enricher_config)
            if enricher is None:
                raise ValueError(f"Unknown enricher type: {enricher_type}")
            self._enricher_cache[enricher_type] = enricher

        return self._enricher_cache[enricher_type]

    def validate_config(self) -> list[str]:
        """Validate step configuration.

        Returns:
            List of validation error messages
        """
        errors = []

        enricher_type = self.config.get("enricher", "auto")
        if enricher_type not in ("auto", "llm", "basic"):
            errors.append(f"Unknown enricher type: {enricher_type}")

        return errors

    def execute(self, context: ExecutionContext) -> StepResult:
        """Execute the enrichment step.

        Reads parsed document from parsed-zone, enriches it, and stores
        the enriched document in enrichment-zone.

        Args:
            context: Execution context with asset, storage, etc.

        Returns:
            StepResult with status and output
        """
        started_at = datetime.now(timezone.utc)

        try:
            # Get parse output (source parsed document)
            parse_output = context.get_step_output("parse")
            if not parse_output:
                return StepResult(
                    status=StepStatus.FAILED,
                    started_at=started_at,
                    completed_at=datetime.now(timezone.utc),
                    error="No parse output found. Enrichment step requires parse step to run first.",
                )

            # Get parsed document path
            parsed_path = parse_output.get("object_path", "")
            if not parsed_path:
                return StepResult(
                    status=StepStatus.FAILED,
                    started_at=started_at,
                    completed_at=datetime.now(timezone.utc),
                    error="Parse output missing object_path.",
                )

            # Build output path
            output_path = self._build_output_path(context)

            # Check if we should skip
            if self.config.get("skip_if_exists", False):
                storage = context.storage
                if storage.object_exists(output_path):
                    return StepResult(
                        status=StepStatus.SKIPPED,
                        started_at=started_at,
                        completed_at=datetime.now(timezone.utc),
                        output={
                            "object_path": output_path,
                            "source_path": parsed_path,
                            "skipped": True,
                            "reason": "Enriched document already exists",
                        },
                    )

            # Read parsed document
            storage = context.storage
            parsed_data = storage.get_object(parsed_path)
            parsed_document = json.loads(parsed_data.decode("utf-8"))

            # Get enricher
            enricher_type = self.config.get("enricher", "auto")
            enricher = self._get_enricher(enricher_type)

            # Enrich document
            enrichment_result = enricher.enrich(parsed_document)

            # Build enriched document
            enriched_document = self._build_enriched_document(
                context=context,
                parsed_document=parsed_document,
                enrichment_result=enrichment_result,
                output_path=output_path,
            )

            # Store enriched document
            self._store_enriched_document(
                context=context,
                enriched_document=enriched_document,
                output_path=output_path,
            )

            # Build output
            output = {
                "object_path": output_path,
                "source_path": parsed_path,
                "enricher": enrichment_result.info.enricher,
                "model": enrichment_result.info.model,
                "zone": ENRICHMENT_ZONE,
                "processing_time_ms": enrichment_result.info.processing_time_ms,
                "tokens_used": enrichment_result.info.tokens_used,
                "cost": enrichment_result.info.cost,
                "entity_count": len(enrichment_result.document.entities),
                "topic_count": len(enrichment_result.document.key_topics),
                "table_enrichment_count": len(enrichment_result.tables),
            }

            return StepResult(
                status=StepStatus.SUCCESS,
                started_at=started_at,
                completed_at=datetime.now(timezone.utc),
                output=output,
            )

        except EnricherError as e:
            return StepResult(
                status=StepStatus.FAILED,
                started_at=started_at,
                completed_at=datetime.now(timezone.utc),
                error=str(e),
            )
        except Exception as e:
            return StepResult(
                status=StepStatus.FAILED,
                started_at=started_at,
                completed_at=datetime.now(timezone.utc),
                error=f"Unexpected error: {e}",
            )

    def _build_output_path(self, context: ExecutionContext) -> str:
        """Build output path for enriched document."""
        execution_time = context.execution_time
        agency_name = context.agency.metadata.name
        asset_name = context.asset.metadata.name

        datestamp = execution_time.strftime("%Y-%m-%d")
        timestamp = execution_time.strftime("%H%M%S")

        return f"{ENRICHMENT_ZONE}/{agency_name}/{asset_name}/{datestamp}/{timestamp}/{asset_name}.json"

    def _build_enriched_document(
        self,
        context: ExecutionContext,
        parsed_document: dict[str, Any],
        enrichment_result: Any,
        output_path: str,
    ) -> dict[str, Any]:
        """Build enriched document combining parsed document with enrichment.

        Args:
            context: Execution context
            parsed_document: Original parsed document
            enrichment_result: Result from enricher
            output_path: Path where enriched document will be stored

        Returns:
            Enriched document dictionary
        """
        # Start with parsed document structure
        enriched_document = {
            "$schema": ENRICHED_DOCUMENT_SCHEMA,
            "metadata": parsed_document.get("metadata", {}),
            "source": {
                **parsed_document.get("source", {}),
                "enrichedStorageUrl": output_path,
            },
            "content": parsed_document.get("content", {}),
            "extraction": parsed_document.get("extraction", {}),
        }

        # Add quality if present
        if "quality" in parsed_document:
            enriched_document["quality"] = parsed_document["quality"]

        # Add enrichment section
        enriched_document["enrichment"] = enrichment_result.to_dict()

        # Generate and add embedding for semantic search
        self._add_embedding(enriched_document)

        return enriched_document

    def _add_embedding(self, enriched_document: dict[str, Any]) -> None:
        """Add embedding vector to enriched document for semantic search.

        The embedding is generated from composite text combining:
        - Document title and summary
        - Key topics and entities
        - Section summaries and table descriptions
        - Example queries

        Args:
            enriched_document: Document to add embedding to (modified in place)
        """
        try:
            from services.embeddings import (
                get_embedding,
                build_composite_text_for_embedding,
                EMBEDDING_DIMENSION,
                MODEL_NAME,
            )

            # Build composite text from enriched content
            composite_text = build_composite_text_for_embedding(enriched_document)

            if not composite_text.strip():
                logger.warning("[enrichment] No text content for embedding generation")
                return

            # Generate embedding
            embedding = get_embedding(composite_text)

            if embedding is not None:
                # Add embedding to enrichment block
                enriched_document["enrichment"]["embedding"] = {
                    "vector": embedding,
                    "model": MODEL_NAME,
                    "dimension": EMBEDDING_DIMENSION,
                }
                logger.info(
                    f"[enrichment] Added embedding ({EMBEDDING_DIMENSION} dims) "
                    f"from {len(composite_text)} chars of text"
                )
            else:
                logger.warning(
                    "[enrichment] Embedding generation skipped "
                    "(sentence-transformers may not be installed)"
                )
        except ImportError:
            logger.debug(
                "[enrichment] Embedding skipped - sentence-transformers not available"
            )
        except Exception as e:
            logger.warning(f"[enrichment] Embedding generation failed: {e}")

    def _store_enriched_document(
        self,
        context: ExecutionContext,
        enriched_document: dict[str, Any],
        output_path: str,
    ) -> None:
        """Store enriched document in enrichment-zone.

        Args:
            context: Execution context
            enriched_document: Enriched document to store
            output_path: Path to store at
        """
        storage = context.storage

        # Serialize to JSON
        json_data = json.dumps(enriched_document, indent=2, ensure_ascii=False).encode("utf-8")

        # Build metadata
        enrichment_info = enriched_document.get("enrichment", {}).get("enrichmentInfo", {})
        metadata = {
            "schema": ENRICHED_DOCUMENT_SCHEMA,
            "source_path": enriched_document["source"].get("parsedStorageUrl", ""),
            "enricher": enrichment_info.get("enricher", "unknown"),
            "workflow": context.plan.workflow_name,
            "run_id": context.master_utid or context.run_id,
            "zone": ENRICHMENT_ZONE,
        }

        if enrichment_info.get("model"):
            metadata["model"] = enrichment_info["model"]

        # Upload to storage
        storage.put_object(
            object_name=output_path,
            data=json_data,
            content_type="application/json",
            metadata=metadata,
        )
