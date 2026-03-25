"""Manifest Executor v1 — bridge between workflow orchestration and interpretation.

Adapted from model_D/mda/orchestrator/manifest_executor/v1/manifest_executor.py.

Execution hierarchy:
    WorkflowExecutor -> ManifestExecutor -> Interpreter -> Capabilities
"""

import json
from typing import Any

from mda.constants import STATUS_SUCCESS, STATUS_FAILED
from mda.traceability.utils import mint_utid
from mda.interpreter.standard_interpreter import StandardInterpreter
from runtime.context import ExecutionContext


class ManifestExecutor:
    """Executes a single manifest by delegating to the StandardInterpreter.

    In the hybrid architecture, ManifestExecutor:
    1. Receives a pre-compiled execution plan and context
    2. Creates a LegacyPipelineParser for the workflow manifest
    3. Wires the parser + context into the StandardInterpreter
    4. Executes and returns results
    """

    def __init__(self) -> None:
        """Initialize ManifestExecutor."""
        pass

    def execute(
        self,
        master_utid: str | None,
        manifest_urn: str,
        context: ExecutionContext,
        parser: Any,
    ) -> dict[str, Any]:
        """Execute a manifest via the interpreter chain.

        Args:
            master_utid: Master UTID (minted if None).
            manifest_urn: URN of the manifest being executed.
            context: Hybrid ExecutionContext (with plan, storage, UTID).
            parser: ParserInterface implementation for this manifest.

        Returns:
            Execution result dict.
        """
        if master_utid is None:
            master_utid = mint_utid()

        try:
            # Create interpreter and inject parser + context
            interpreter = StandardInterpreter(master_utid, manifest_urn)
            interpreter.set_parser_and_context(parser, context)

            # Execute
            result = interpreter.execute()
            return result

        except Exception as e:
            result = {
                "status": STATUS_FAILED,
                "utid": master_utid,
                "manifest_urn": manifest_urn,
                "error": str(e),
                "error_type": type(e).__name__,
            }
            return result
