"""Interpreter Interface — contract for all manifest interpreters.

Ported from model_D/mda/provider/interpreter/interpreter_interface.py.
"""

from abc import ABC, abstractmethod
from typing import Any


class InterpreterInterface(ABC):
    """Interface that all interpreters must implement.

    An interpreter receives a manifest URN and a UTID, loads the manifest,
    resolves capabilities, and executes them sequentially.
    """

    @abstractmethod
    def __init__(self, master_utid: str, manifest_urn: str) -> None:
        """Initialize interpreter with execution identity.

        Args:
            master_utid: Universal Trace ID for lineage.
            manifest_urn: Manifest URN to execute.
        """
        pass

    @abstractmethod
    def execute(self) -> dict[str, Any]:
        """Execute the manifest and return results.

        Returns:
            Result dict with status, utid, manifest_id, steps_executed, results.
        """
        pass
