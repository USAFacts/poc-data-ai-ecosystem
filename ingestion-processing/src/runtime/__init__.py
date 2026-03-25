"""Runtime - Polymorphic execution engine for compiled plans."""

from runtime.context import ExecutionContext
from runtime.executor import Executor, ExecutionResult
from runtime.sequential import SequentialExecutor

__all__ = [
    "ExecutionContext",
    "Executor",
    "ExecutionResult",
    "SequentialExecutor",
]
