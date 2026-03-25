"""CapabilityAdapter — bridges existing Step subclasses to CapabilityInterface.

This adapter allows all existing steps (AcquisitionStep, ParseStep, EnrichmentStep)
to be used through the MDA execution path without rewriting their logic.
"""

from typing import Any, TYPE_CHECKING

from mda.capability.interface import CapabilityInterface

if TYPE_CHECKING:
    from runtime.context import ExecutionContext
    from steps.base import Step


class CapabilityAdapter(CapabilityInterface):
    """Wraps an existing Step subclass as a CapabilityInterface.

    The adapter:
    1. Instantiates the Step with name and config from params
    2. Delegates execute() to step.execute(context)
    3. Converts StepResult to a plain dict

    This is the bridge between the legacy step system and Model_D's
    capability-based execution.
    """

    def __init__(
        self,
        context: "ExecutionContext",
        params: dict[str, Any],
        step_class: type["Step"],
    ) -> None:
        """Initialize adapter.

        Args:
            context: Execution context.
            params: Manifest params (must include 'step_name', may include 'config').
            step_class: The Step subclass to wrap.
        """
        self._step_class = step_class
        # Call parent which triggers validate_params
        super().__init__(context, params)

    def validate_params(self) -> bool:
        """Validate that step_name is provided in params."""
        return "step_name" in self.params

    def execute(self) -> dict[str, Any]:
        """Execute the wrapped step and convert result to dict.

        Returns:
            Dict with keys: status, started_at, completed_at, duration_seconds,
            output, error — matching StepResult.to_dict() shape.
        """
        step_name = self.params["step_name"]
        step_config = self.params.get("config", {})

        # Instantiate the legacy step
        step = self._step_class(name=step_name, config=step_config)

        self.log(f"Executing legacy step: {step}")

        # Execute and convert result
        result = step.execute(self.context)
        return result.to_dict()
