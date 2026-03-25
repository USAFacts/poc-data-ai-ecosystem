"""CapabilityInterface — the universal contract for all MDA capabilities.

Ported from model_D/mda/provider/capability/v1/capability_interface.py.
Every capability (in-process step, MCP network call, etc.) implements this ABC.
"""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from logging_manager import get_logger

if TYPE_CHECKING:
    from runtime.context import ExecutionContext

_logger = get_logger(__name__)


class CapabilityInterface(ABC):
    """Abstract base for all MDA capabilities.

    A capability is a unit of work that:
    - Receives an ExecutionContext (system-provided) and params (manifest-provided)
    - Validates its parameters on construction
    - Executes and returns a result dict

    This is the Model_D equivalent of the existing Step ABC, but decoupled
    from pipeline-specific concerns (no step_type, no StepResult dataclass).
    """

    def __init__(self, context: "ExecutionContext", params: dict[str, Any]) -> None:
        """Initialize capability.

        Args:
            context: Execution context with traceability, storage, and step results.
            params: Parameters from the manifest step definition.

        Raises:
            ValueError: If validate_params() returns False.
        """
        self.context = context
        self.params = params

        if not self.validate_params():
            raise ValueError(
                f"Invalid capability params for {self.__class__.__name__}: {params}"
            )

    @abstractmethod
    def validate_params(self) -> bool:
        """Validate that manifest-provided params are sufficient for execution.

        Returns:
            True if params are valid, False otherwise.
        """
        pass

    @abstractmethod
    def execute(self) -> dict[str, Any]:
        """Execute the capability's core logic.

        Returns:
            Result dictionary with capability-specific output.
        """
        pass

    def log(self, message: str, level: str = "INFO") -> None:
        """Structured logging for observability.

        Args:
            message: Log message.
            level: Log level (INFO, WARN, ERROR, DEBUG).
        """
        utid = getattr(self.context, "master_utid", "no-utid")
        log_fn = getattr(_logger, level.lower(), _logger.info)
        log_fn(message, extra={"utid": utid, "capability": self.__class__.__name__})

    def trace(self) -> None:
        """Write execution evidence to the Evidence Store.

        Not yet implemented — placeholder for future traceability integration.
        """
        raise NotImplementedError(
            "Trace method should be implemented by the MDA system, not the provider."
        )
