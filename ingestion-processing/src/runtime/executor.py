"""Abstract executor interface for runtime polymorphism."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from control.compiler import ExecutionPlan
from runtime.context import ExecutionContext
from steps.base import StepResult


@dataclass
class ExecutionResult:
    """Result of executing an execution plan."""

    plan_id: str
    workflow_name: str
    run_id: str
    started_at: datetime
    completed_at: datetime | None = None
    status: str = "running"
    step_results: dict[str, StepResult] = field(default_factory=dict)
    error: str | None = None
    master_utid: str = ""

    @property
    def success(self) -> bool:
        """Check if execution was successful (or dry_run completed)."""
        return self.status in ("success", "dry_run")

    @property
    def duration_seconds(self) -> float | None:
        """Get execution duration in seconds."""
        if self.completed_at is None:
            return None
        return (self.completed_at - self.started_at).total_seconds()

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        result = {
            "plan_id": self.plan_id,
            "workflow_name": self.workflow_name,
            "run_id": self.run_id,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": self.duration_seconds,
            "status": self.status,
            "step_results": {
                name: sr.to_dict() for name, sr in self.step_results.items()
            },
            "error": self.error,
        }
        if self.master_utid:
            result["master_utid"] = self.master_utid
        return result


class Executor(ABC):
    """Abstract base class for execution engines.

    Subclasses implement different execution strategies:
    - SequentialExecutor: Runs steps one at a time
    - ParallelExecutor: Runs independent steps concurrently
    - DistributedExecutor: Runs steps across workers
    """

    @abstractmethod
    def execute(
        self,
        plan: ExecutionPlan,
        context: ExecutionContext,
        dry_run: bool = False,
    ) -> ExecutionResult:
        """Execute a compiled plan.

        Args:
            plan: Compiled execution plan from Compiler
            context: Execution context with storage and state
            dry_run: If True, validate but don't execute

        Returns:
            ExecutionResult with status and step outputs
        """
        pass

    @abstractmethod
    def execute_step(
        self,
        step_name: str,
        plan: ExecutionPlan,
        context: ExecutionContext,
    ) -> StepResult:
        """Execute a single step.

        Args:
            step_name: Name of step to execute
            plan: The execution plan
            context: Execution context

        Returns:
            StepResult from step execution
        """
        pass
