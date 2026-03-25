"""Workflow Executor v1 — orchestrates multi-manifest workflow execution.

Adapted from model_D/mda/orchestrator/workflow_executor/v1/default_workflow_executor.py.

Execution hierarchy:
    WorkflowExecutor -> ManifestExecutor -> Interpreter -> Capabilities
"""

from typing import Any

from mda.constants import STATUS_SUCCESS, STATUS_FAILED
from mda.traceability.utils import mint_utid
from mda.orchestrator.manifest_executor import ManifestExecutor
from mda.manifest.urn import build_manifest_urn
from runtime.context import ExecutionContext


class DefaultWorkflowExecutor:
    """Executes a workflow by delegating to ManifestExecutor.

    In the hybrid architecture, a single workflow maps to a single manifest
    (the workflow YAML). The WorkflowExecutor:
    1. Mints a master UTID for traceability
    2. Builds the manifest URN from the execution plan
    3. Creates the parser and context
    4. Delegates to ManifestExecutor

    In future, multi-manifest workflows (e.g., curation -> semantics)
    will iterate through multiple manifest URNs.
    """

    def __init__(self) -> None:
        """Initialize WorkflowExecutor."""
        self.manifest_executor = ManifestExecutor()

    def execute(
        self,
        context: ExecutionContext,
        parser: Any,
        manifest_urn: str | None = None,
    ) -> dict[str, Any]:
        """Execute a workflow.

        Args:
            context: Hybrid ExecutionContext (with plan, storage).
            parser: ParserInterface implementation for the workflow.
            manifest_urn: Optional manifest URN. Built from plan if not provided.

        Returns:
            Execution result dict.
        """
        # Mint master UTID for entire workflow
        master_utid = mint_utid()

        # Build manifest URN if not provided
        if manifest_urn is None:
            plan = context.plan
            agency_labels = plan.agency.metadata.labels
            domain = agency_labels.get("category", "general")
            manifest_urn = build_manifest_urn(
                layer="curation",
                domain=domain,
                path=f"{plan.agency.metadata.name}/{plan.asset.metadata.name}",
                version="1.0.0",
            )

        # Set traceability fields on context
        context.master_utid = master_utid
        context.manifest_urn = manifest_urn

        # Delegate to ManifestExecutor
        result = self.manifest_executor.execute(
            master_utid=master_utid,
            manifest_urn=manifest_urn,
            context=context,
            parser=parser,
        )

        # Add workflow-level metadata
        result["master_utid"] = master_utid
        result["workflow_name"] = context.plan.workflow_name

        return result
