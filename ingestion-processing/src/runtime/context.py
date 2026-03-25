"""Execution context for runtime.

Hybrid context that supports both the legacy step API (set_step_output/get_step_output)
and the Model_D API (append_result/get_result/get_results) through a shared data store.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from control.compiler import ExecutionPlan
from control.models import Agency, Asset, Workflow


@dataclass
class ExecutionContext:
    """Context passed to steps during execution.

    Contains all information a step needs to execute:
    - The compiled execution plan
    - Resolved workflow, asset, and agency
    - Storage client for persisting results
    - Step outputs from previous steps

    MDA extensions (Model_D):
    - master_utid: Universal Trace ID linking all artifacts in a run
    - manifest_urn: URN of the manifest being executed
    - append_result/get_result/get_results: Model_D result API
    """

    plan: ExecutionPlan
    storage: Any  # MinioStorage - avoid circular import
    execution_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    run_id: str = ""
    step_outputs: dict[str, dict[str, Any]] = field(default_factory=dict)

    # --- MDA extensions (backward-compatible defaults) ---
    master_utid: str = ""
    manifest_urn: str = ""

    def __post_init__(self) -> None:
        """Generate run ID if not provided.

        When master_utid is provided, use it as the run_id prefix
        for traceability. Otherwise fall back to timestamp.
        """
        if not self.run_id:
            if self.master_utid:
                self.run_id = self.master_utid
            else:
                self.run_id = self.execution_time.strftime("%Y%m%d_%H%M%S")

    @property
    def workflow(self) -> Workflow:
        """Get the workflow being executed."""
        # Reconstruct minimal workflow from plan
        # In practice, we use the plan directly
        from control.models import Workflow, WorkflowSpec, Metadata, StepConfig

        return Workflow(
            metadata=Metadata(name=self.plan.workflow_name, labels={}),
            spec=WorkflowSpec(
                assetRef=self.plan.asset.metadata.name,
                steps=[
                    StepConfig(
                        name=s.name,
                        type=s.type,
                        config=s.config,
                        dependsOn=s.dependencies,
                    )
                    for s in self.plan.steps
                ],
            ),
        )

    @property
    def asset(self) -> Asset:
        """Get the asset being processed."""
        return self.plan.asset

    @property
    def agency(self) -> Agency:
        """Get the agency that owns the asset."""
        return self.plan.agency

    def get_step_output(self, step_name: str) -> dict[str, Any] | None:
        """Get output from a previously executed step.

        Args:
            step_name: Name of the step

        Returns:
            Step output dict or None if not found
        """
        return self.step_outputs.get(step_name)

    def set_step_output(self, step_name: str, output: dict[str, Any]) -> None:
        """Store output from a step.

        Args:
            step_name: Name of the step
            output: Output dictionary to store
        """
        self.step_outputs[step_name] = output

    # --- Model_D API (delegates to step_outputs for shared data store) ---

    def append_result(self, step_name: str, result: Any) -> None:
        """Append step result to context (Model_D API).

        When the result is a StepResult-shaped dict (from CapabilityAdapter),
        unwrap the ``output`` sub-dict so downstream steps can read it
        directly via ``get_step_output()``.

        Args:
            step_name: Name of the step.
            result: Result data (typically a dict from StepResult.to_dict()).
        """
        if isinstance(result, dict) and "output" in result and "status" in result:
            self.step_outputs[step_name] = result["output"]
        else:
            self.step_outputs[step_name] = result

    def get_result(self, step_name: str) -> dict[str, Any] | None:
        """Get result from a previous step (Model_D API).

        Delegates to get_step_output.

        Args:
            step_name: Name of the step.

        Returns:
            Step result or None if not found.
        """
        return self.step_outputs.get(step_name)

    def get_results(self) -> dict[str, dict[str, Any]]:
        """Get all step results (Model_D API).

        Returns:
            Copy of all step outputs.
        """
        return self.step_outputs.copy()

    # --- Serialization ---

    def to_dict(self) -> dict[str, Any]:
        """Convert context to dictionary for serialization."""
        result = {
            "workflow": self.plan.workflow_name,
            "asset": self.asset.metadata.name,
            "agency": self.agency.metadata.name,
            "execution_time": self.execution_time.isoformat(),
            "run_id": self.run_id,
            "step_outputs": self.step_outputs,
        }
        if self.master_utid:
            result["master_utid"] = self.master_utid
        if self.manifest_urn:
            result["manifest_urn"] = self.manifest_urn
        return result
