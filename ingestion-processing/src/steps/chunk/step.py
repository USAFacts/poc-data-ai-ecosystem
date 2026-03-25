"""Chunk step implementation — splits parsed documents into hierarchical chunks."""

import json
from datetime import datetime, timezone
from typing import Any

from logging_manager import get_logger
from runtime.context import ExecutionContext
from steps.base import Step, StepResult, StepStatus
from steps.chunk.chunkers import ChunkerError, HierarchicalChunker
from storage.naming import CHUNK_ZONE

logger = get_logger(__name__)

CHUNKED_DOCUMENT_SCHEMA = "https://usafacts.org/schemas/chunked-document/v1"


class ChunkStep(Step):
    """Step that splits parsed documents into hierarchical chunks.

    Reads a parsed document from parsed-zone, decomposes it into
    document-level, section-level, and table-level chunks, then
    stores the result in chunk-zone.

    This step should run after parse and before enrich so that
    enrichment can reference individual chunk IDs.
    """

    step_type = "chunk"

    def execute(self, context: ExecutionContext) -> StepResult:
        """Execute the chunk step.

        Args:
            context: Execution context with asset, storage, etc.

        Returns:
            StepResult with chunk counts and storage path.
        """
        started_at = datetime.now(timezone.utc)
        run_id = context.master_utid or context.run_id

        try:
            # Get parse output
            parse_output = context.get_step_output("parse")
            if not parse_output:
                return StepResult(
                    status=StepStatus.FAILED,
                    started_at=started_at,
                    completed_at=datetime.now(timezone.utc),
                    error="No parse output found. Chunk step requires parse step to run first.",
                    run_id=run_id,
                )

            source_path = parse_output.get("object_path", "")
            if not source_path:
                return StepResult(
                    status=StepStatus.SKIPPED,
                    started_at=started_at,
                    completed_at=datetime.now(timezone.utc),
                    error="Parse output has no object_path. Acquisition may have failed.",
                    run_id=run_id,
                )

            storage = context.storage

            # Read parsed document from MinIO
            data = storage.get_object(source_path)
            parsed_document = json.loads(data.decode("utf-8"))

            # Run hierarchical chunker
            chunker = HierarchicalChunker()
            chunk_result = chunker.chunk(parsed_document)

            logger.info(
                "Document chunked",
                extra={
                    "workflow": context.plan.workflow_name,
                    "step": "chunk",
                    "run_id": run_id,
                    "asset": context.asset.metadata.name,
                },
            )

            # Build chunked document envelope
            chunked_document = {
                "$schema": CHUNKED_DOCUMENT_SCHEMA,
                "document_id": chunk_result.document_id,
                "source_path": source_path,
                "run_id": run_id,
                "chunk_count": chunk_result.total_chunks,
                "chunks": [c.to_dict() for c in chunk_result.chunks],
            }

            # Store in chunk-zone
            output_path = self._store_chunked_document(context, chunked_document)

            completed_at = datetime.now(timezone.utc)
            output = {
                "object_path": output_path,
                "source_path": source_path,
                "chunk_count": chunk_result.total_chunks,
                "document_chunks": sum(1 for c in chunk_result.chunks if c.level == "document"),
                "section_chunks": sum(1 for c in chunk_result.chunks if c.level == "section"),
                "table_chunks": sum(1 for c in chunk_result.chunks if c.level == "table"),
                "zone": CHUNK_ZONE,
            }

            return StepResult(
                status=StepStatus.SUCCESS,
                started_at=started_at,
                completed_at=completed_at,
                output=output,
                run_id=run_id,
            )

        except ChunkerError as e:
            logger.error(
                "Chunking failed",
                extra={"workflow": context.plan.workflow_name, "step": "chunk", "run_id": run_id},
            )
            return StepResult(
                status=StepStatus.FAILED,
                started_at=started_at,
                completed_at=datetime.now(timezone.utc),
                error=str(e),
                run_id=run_id,
            )
        except Exception as e:
            import traceback
            logger.error(
                f"Unexpected error in chunk step: {e}\n{traceback.format_exc()}",
                extra={"workflow": context.plan.workflow_name, "step": "chunk", "run_id": run_id},
            )
            return StepResult(
                status=StepStatus.FAILED,
                started_at=started_at,
                completed_at=datetime.now(timezone.utc),
                error=f"Unexpected error: {e}",
                run_id=run_id,
            )

    def _store_chunked_document(
        self,
        context: ExecutionContext,
        chunked_document: dict[str, Any],
    ) -> str:
        """Store chunked document in chunk-zone."""
        storage = context.storage
        execution_time = context.execution_time
        agency_name = context.agency.metadata.name
        asset_name = context.asset.metadata.name

        datestamp = execution_time.strftime("%Y-%m-%d")
        timestamp = execution_time.strftime("%H%M%S")
        object_path = f"{CHUNK_ZONE}/{agency_name}/{asset_name}/{datestamp}/{timestamp}/{asset_name}_chunks.json"

        json_data = json.dumps(chunked_document, indent=2, ensure_ascii=False).encode("utf-8")

        metadata = {
            "schema": CHUNKED_DOCUMENT_SCHEMA,
            "source_path": chunked_document["source_path"],
            "chunk_count": str(chunked_document["chunk_count"]),
            "workflow": context.plan.workflow_name,
            "run_id": context.master_utid or context.run_id,
            "zone": CHUNK_ZONE,
        }

        storage.put_object(
            object_name=object_path,
            data=json_data,
            content_type="application/json",
            metadata=metadata,
        )

        return object_path
