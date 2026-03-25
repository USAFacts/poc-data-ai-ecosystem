"""Parser Interface — contract for all manifest parsers.

Ported from model_D/mda/manifest/parser_interface.py.
Each manifest schema (pipeline/v1, standard/1.0, etc.) gets its own parser
that implements this interface.
"""

from abc import ABC, abstractmethod
from typing import Any


class ParserInterface(ABC):
    """Interface that all manifest parsers must implement.

    A parser reads a manifest (from a file path or dict) and provides
    a uniform API for the interpreter to extract execution information.
    """

    # Subclasses should declare which interpreter to use
    INTERPRETER_PATH: str = "mda.interpreter.standard_interpreter"

    @abstractmethod
    def __init__(self, manifest_path: str) -> None:
        """Initialize parser with a manifest file path.

        Args:
            manifest_path: Path to the manifest YAML file.
        """
        pass

    @abstractmethod
    def parse(self) -> None:
        """Parse and validate manifest content."""
        pass

    @abstractmethod
    def get_manifest_id(self) -> str:
        """Get unique manifest identifier."""
        pass

    @abstractmethod
    def get_manifest_version(self) -> str:
        """Get manifest version string."""
        pass

    @abstractmethod
    def get_provider(self) -> str:
        """Get provider name (e.g., 'mda_ingestion_provider')."""
        pass

    @abstractmethod
    def get_engine(self) -> str:
        """Get engine name (e.g., 'python_v0')."""
        pass

    @abstractmethod
    def get_steps(self) -> list[dict[str, Any]]:
        """Get list of step definitions from the manifest."""
        pass

    @abstractmethod
    def get_step_name(self, step: dict[str, Any]) -> str:
        """Extract step name from a step definition."""
        pass

    @abstractmethod
    def get_step_component_path(self, step: dict[str, Any]) -> str:
        """Extract component path from a step definition."""
        pass

    @abstractmethod
    def get_step_component_params(self, step: dict[str, Any]) -> dict[str, Any]:
        """Extract component params from a step definition."""
        pass
