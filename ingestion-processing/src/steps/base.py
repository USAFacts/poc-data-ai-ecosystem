"""Base class for pipeline steps."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from runtime.context import ExecutionContext


class StepStatus(str, Enum):
    """Status of a step execution."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class StepResult:
    """Result of a step execution."""

    status: StepStatus
    started_at: datetime
    completed_at: datetime | None = None
    output: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    run_id: str = ""

    @property
    def duration_seconds(self) -> float | None:
        """Get execution duration in seconds."""
        if self.completed_at is None:
            return None
        return (self.completed_at - self.started_at).total_seconds()

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        result = {
            "status": self.status.value,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": self.duration_seconds,
            "output": self.output,
            "error": self.error,
        }
        if self.run_id:
            result["run_id"] = self.run_id
        return result


class Step(ABC):
    """Abstract base class for pipeline steps.

    Subclasses must implement the execute() method to perform
    the actual step logic.
    """

    # Step type identifier (override in subclasses)
    step_type: str = "base"

    def __init__(self, name: str, config: dict[str, Any] | None = None) -> None:
        """Initialize step.

        Args:
            name: Name of this step instance
            config: Step-specific configuration
        """
        self.name = name
        self.config = config or {}

    @abstractmethod
    def execute(self, context: "ExecutionContext") -> StepResult:
        """Execute the step.

        Args:
            context: Execution context with workflow info and storage

        Returns:
            StepResult with status and output
        """
        pass

    def validate_config(self) -> list[str]:
        """Validate step configuration.

        Override in subclasses to add validation logic.

        Returns:
            List of validation error messages (empty if valid)
        """
        return []

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r}, type={self.step_type!r})"
