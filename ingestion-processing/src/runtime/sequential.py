"""Sequential executor implementation.

Executes workflows through the MDA interpreter chain:
    Compiler -> ExecutionPlan -> PlanBasedParser -> StandardInterpreter -> DefaultResolver -> Capabilities
"""

from datetime import datetime, timezone
from typing import Any

from rich.console import Console

from control.compiler import ExecutionPlan
from logging_manager import get_logger
from runtime.context import ExecutionContext
from runtime.executor import Executor, ExecutionResult
from steps.base import StepResult, StepStatus

logger = get_logger(__name__)


class SequentialExecutor(Executor):
    """Executes steps sequentially via the MDA interpreter chain."""

    def __init__(self, console: Console | None = None) -> None:
        """Initialize executor.

        Args:
            console: Rich console for output (default: create new)
        """
        self.console = console or Console()

    def execute(
        self,
        plan: ExecutionPlan,
        context: ExecutionContext,
        dry_run: bool = False,
        show_progress: bool = True,
    ) -> ExecutionResult:
        """Execute a compiled plan sequentially.

        Args:
            plan: Compiled execution plan
            context: Execution context
            dry_run: If True, validate but don't execute
            show_progress: If True, show progress spinner

        Returns:
            ExecutionResult with status and outputs
        """
        started_at = datetime.now(timezone.utc)

        result = ExecutionResult(
            plan_id=plan.plan_id,
            workflow_name=plan.workflow_name,
            run_id=context.run_id,
            started_at=started_at,
        )

        if dry_run:
            self.console.print(
                f"[yellow]Dry run - would execute: {plan.execution_order}[/yellow]"
            )
            logger.info("Dry run completed", extra={"workflow": plan.workflow_name})
            result.status = "dry_run"
            result.completed_at = datetime.now(timezone.utc)
            return result

        logger.info(
            "Workflow execution started",
            extra={"workflow": plan.workflow_name, "run_id": context.run_id},
        )

        try:
            self._execute_via_interpreter(plan, context, result)
        except Exception as e:
            result.status = "failed"
            result.error = f"Execution error: {e}"
            logger.error(
                "Workflow execution failed",
                extra={
                    "workflow": plan.workflow_name,
                    "run_id": result.master_utid or context.run_id,
                    "error": str(e),
                },
            )

        result.completed_at = datetime.now(timezone.utc)

        if result.status != "failed":
            logger.info(
                "Workflow execution completed",
                extra={
                    "workflow": plan.workflow_name,
                    "run_id": result.master_utid or context.run_id,
                    "status": result.status,
                },
            )

        return result

    def execute_step(
        self,
        step_name: str,
        plan: ExecutionPlan,
        context: ExecutionContext,
    ) -> StepResult:
        """Execute a single step via the MDA resolver chain.

        Args:
            step_name: Name of step to execute
            plan: The execution plan
            context: Execution context

        Returns:
            StepResult from step execution
        """
        from mda.resolver.default_resolver import DefaultResolver
        from mda.traceability.utils import mint_utid

        # Mint a UTID for this step execution if one isn't set on context
        if not context.master_utid:
            context.master_utid = mint_utid()
        run_id = context.master_utid

        exec_step = plan.get_step(step_name)
        if exec_step is None:
            logger.error("Step not found", extra={"workflow": plan.workflow_name, "step": step_name, "run_id": run_id})
            return StepResult(
                status=StepStatus.FAILED,
                started_at=datetime.now(timezone.utc),
                completed_at=datetime.now(timezone.utc),
                error=f"Step not found: {step_name}",
                run_id=run_id,
            )

        component_path = _STEP_TYPE_MAP.get(exec_step.type)
        if component_path is None:
            logger.error("Unknown step type", extra={"workflow": plan.workflow_name, "step": step_name, "run_id": run_id})
            return StepResult(
                status=StepStatus.FAILED,
                started_at=datetime.now(timezone.utc),
                completed_at=datetime.now(timezone.utc),
                error=f"Unknown step type: {exec_step.type}",
                run_id=run_id,
            )

        logger.info(
            "Step started",
            extra={"workflow": plan.workflow_name, "step": exec_step.type, "run_id": run_id},
        )

        # Resolve capability via DefaultResolver
        resolver = DefaultResolver(provider="mda_ingestion_provider", engine="python_v0")
        CapabilityClass = resolver.resolve(component_path)

        # Build params matching what the adapter expects
        params = dict(exec_step.config)
        params["step_name"] = exec_step.name

        # Instantiate and execute
        capability = CapabilityClass(context=context, params=params)
        result_dict = capability.execute()

        step_status = result_dict.get("status", "success")
        if step_status == "success":
            logger.info(
                "Step completed",
                extra={"workflow": plan.workflow_name, "step": exec_step.type, "run_id": run_id},
            )
        else:
            logger.error(
                "Step failed",
                extra={
                    "workflow": plan.workflow_name,
                    "step": exec_step.type,
                    "run_id": run_id,
                    "error": result_dict.get("error", ""),
                },
            )

        # Convert dict back to StepResult
        return StepResult(
            status=StepStatus(step_status),
            started_at=datetime.fromisoformat(result_dict["started_at"])
            if "started_at" in result_dict
            else datetime.now(timezone.utc),
            completed_at=datetime.fromisoformat(result_dict["completed_at"])
            if "completed_at" in result_dict
            else datetime.now(timezone.utc),
            output=result_dict.get("output", result_dict),
            error=result_dict.get("error"),
            run_id=run_id,
        )

    def _execute_via_interpreter(
        self,
        plan: ExecutionPlan,
        context: ExecutionContext,
        result: ExecutionResult,
    ) -> None:
        """Execute using the MDA interpreter chain."""
        from mda.orchestrator.workflow_executor import DefaultWorkflowExecutor

        parser = _PlanBasedParser(plan)
        parser.parse()

        workflow_executor = DefaultWorkflowExecutor()
        mda_result = workflow_executor.execute(context=context, parser=parser)

        self._convert_mda_result(mda_result, result, plan)

    def _convert_mda_result(
        self,
        mda_result: dict[str, Any],
        result: ExecutionResult,
        plan: ExecutionPlan,
    ) -> None:
        """Convert MDA interpreter result dict back to ExecutionResult."""
        from mda.constants import STATUS_SUCCESS

        # Propagate UTID from MDA result to ExecutionResult
        master_utid = mda_result.get("master_utid", "")
        result.master_utid = master_utid

        status = mda_result.get("status", "failed")
        step_results = mda_result.get("results", {})

        for step_name, step_data in step_results.items():
            step_status = StepStatus.SUCCESS
            step_error = None

            if isinstance(step_data, dict):
                raw_status = step_data.get("status", "success")
                if raw_status == "failed" or raw_status == StepStatus.FAILED.value:
                    step_status = StepStatus.FAILED
                    step_error = step_data.get("error")

                step_result = StepResult(
                    status=step_status,
                    started_at=datetime.fromisoformat(step_data["started_at"])
                    if "started_at" in step_data
                    else datetime.now(timezone.utc),
                    completed_at=datetime.fromisoformat(step_data["completed_at"])
                    if "completed_at" in step_data
                    else datetime.now(timezone.utc),
                    output=step_data.get("output", step_data),
                    error=step_error,
                    run_id=master_utid,
                )
            else:
                step_result = StepResult(
                    status=step_status,
                    started_at=datetime.now(timezone.utc),
                    completed_at=datetime.now(timezone.utc),
                    output=step_data if isinstance(step_data, dict) else {},
                    run_id=master_utid,
                )

            result.step_results[step_name] = step_result

        if status == STATUS_SUCCESS:
            result.status = "success"
            self._track_dis(plan.workflow_name, result)
        else:
            result.status = "failed"
            result.error = mda_result.get("error", "Execution failed")

    def _track_dis(self, workflow_name: str, result: ExecutionResult) -> None:
        """Track DIS score for a completed workflow execution."""
        try:
            from runtime.dis_tracker import track_workflow_dis, update_overall_dis

            track_workflow_dis(workflow_name, result)
            update_overall_dis()
        except Exception:
            pass


# --- Step type to component path mapping (shared) ---

_STEP_TYPE_MAP = {
    "acquisition": "acquisition/v1/default_acquisition",
    "parse": "parse/v1/default_parse",
    "chunk": "chunk/v1/default_chunk",
    "enrichment": "enrichment/v1/default_enrichment",
    "sync": "sync/v1/default_sync",
}


class _PlanBasedParser:
    """Extracts step information from an already-compiled ExecutionPlan.

    Implements the ParserInterface contract so the StandardInterpreter
    can execute steps without re-reading YAML files.
    """

    def __init__(self, plan: ExecutionPlan) -> None:
        self._plan = plan
        self._steps: list[dict[str, Any]] = []

    def parse(self) -> None:
        """Build step list ordered by execution_order."""
        order = {name: i for i, name in enumerate(self._plan.execution_order)}
        self._steps = sorted(
            [
                {
                    "name": step.name,
                    "type": step.type,
                    "config": step.config,
                    "dependencies": step.dependencies,
                }
                for step in self._plan.steps
            ],
            key=lambda s: order.get(s["name"], 999),
        )

    def get_manifest_id(self) -> str:
        return self._plan.workflow_name

    def get_manifest_version(self) -> str:
        return "1.0.0"

    def get_provider(self) -> str:
        return "mda_ingestion_provider"

    def get_engine(self) -> str:
        return "python_v0"

    def get_steps(self) -> list[dict[str, Any]]:
        return self._steps

    def get_step_name(self, step: dict[str, Any]) -> str:
        return step.get("name", "unnamed")

    def get_step_component_path(self, step: dict[str, Any]) -> str:
        step_type = step.get("type", "")
        path = _STEP_TYPE_MAP.get(step_type)
        if not path:
            raise ValueError(f"Unknown step type '{step_type}'")
        return path

    def get_step_component_params(self, step: dict[str, Any]) -> dict[str, Any]:
        params = dict(step.get("config", {}))
        params["step_name"] = step.get("name", "unnamed")
        return params
