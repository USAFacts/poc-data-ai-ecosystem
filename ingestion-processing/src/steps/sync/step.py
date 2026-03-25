"""Sync step — indexes enriched documents into Weaviate and Neo4j.

Runs after the enrich step. Reads the enriched document and chunk data
from MinIO, then pushes them to the search and graph backends so they
are immediately available for queries without a manual refresh.
"""

import json
import os
from datetime import datetime, timezone
from typing import Any

from logging_manager import get_logger
from runtime.context import ExecutionContext
from steps.base import Step, StepResult, StepStatus

logger = get_logger(__name__)


class SyncStep(Step):
    """Step that syncs pipeline outputs to Weaviate and Neo4j.

    Reads:
        - enrichment-zone document (from enrich step output)
        - chunk-zone chunks (from chunk step output)

    Writes to:
        - Weaviate: GovDocument + GovChunk collections
        - Neo4j: Document, Entity, Agency, TimePeriod nodes + relationships
    """

    step_type = "sync"

    def execute(self, context: ExecutionContext) -> StepResult:
        started_at = datetime.now(timezone.utc)
        run_id = context.master_utid or context.run_id

        enrich_output = context.get_step_output("enrich")
        if not enrich_output:
            return StepResult(
                status=StepStatus.FAILED,
                started_at=started_at,
                completed_at=datetime.now(timezone.utc),
                error="No enrich output found. Sync step requires enrich step to run first.",
                run_id=run_id,
            )

        storage = context.storage
        enrich_path = enrich_output.get("object_path", "")

        # Load enriched document
        try:
            data = storage.get_object(enrich_path)
            enriched_doc = json.loads(data.decode("utf-8"))
        except Exception as e:
            return StepResult(
                status=StepStatus.FAILED,
                started_at=started_at,
                completed_at=datetime.now(timezone.utc),
                error=f"Failed to load enriched document: {e}",
                run_id=run_id,
            )

        # Load chunks (optional — may not exist if chunk step was skipped)
        chunks = None
        chunk_output = context.get_step_output("chunk")
        if chunk_output:
            try:
                chunk_data = storage.get_object(chunk_output["object_path"])
                chunk_doc = json.loads(chunk_data.decode("utf-8"))
                chunks = chunk_doc.get("chunks", [])
            except Exception as e:
                logger.warning(
                    "Failed to load chunks, syncing without them",
                    extra={"step": "sync", "run_id": run_id, "error": str(e)},
                )

        weaviate_synced = False
        neo4j_synced = False
        errors: list[str] = []

        # --- Weaviate sync ---
        try:
            from steps.sync.clients import sync_to_weaviate

            sync_to_weaviate(enriched_doc, chunks)
            weaviate_synced = True
            logger.info(
                "Synced to Weaviate",
                extra={
                    "step": "sync",
                    "run_id": run_id,
                    "workflow": context.plan.workflow_name,
                    "chunks": len(chunks) if chunks else 0,
                },
            )
        except Exception as e:
            msg = f"Weaviate sync failed: {e}"
            errors.append(msg)
            logger.warning(msg, extra={"step": "sync", "run_id": run_id})

        # --- Neo4j sync ---
        try:
            from steps.sync.clients import sync_to_neo4j

            sync_to_neo4j(enriched_doc)
            neo4j_synced = True
            logger.info(
                "Synced to Neo4j",
                extra={
                    "step": "sync",
                    "run_id": run_id,
                    "workflow": context.plan.workflow_name,
                },
            )
        except Exception as e:
            msg = f"Neo4j sync failed: {e}"
            errors.append(msg)
            logger.warning(msg, extra={"step": "sync", "run_id": run_id})

        completed_at = datetime.now(timezone.utc)

        # Succeed if at least one backend was synced
        status = StepStatus.SUCCESS if (weaviate_synced or neo4j_synced) else StepStatus.FAILED

        output = {
            "weaviate_synced": weaviate_synced,
            "neo4j_synced": neo4j_synced,
            "chunks_synced": len(chunks) if chunks and weaviate_synced else 0,
            "errors": errors,
        }

        return StepResult(
            status=status,
            started_at=started_at,
            completed_at=completed_at,
            output=output,
            error="; ".join(errors) if errors else None,
            run_id=run_id,
        )
