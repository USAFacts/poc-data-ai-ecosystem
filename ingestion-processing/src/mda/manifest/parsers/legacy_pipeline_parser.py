"""Legacy Pipeline Parser — bridges pipeline/v1 manifests to Model_D.

This parser wraps the existing Registry + Compiler flow and exposes it
through the ParserInterface. It allows existing pipeline/v1 manifests
to be executed through the Model_D interpreter chain.
"""

from typing import Any

import yaml

from mda.manifest.parser_interface import ParserInterface


# Step type to component path mapping
_STEP_TYPE_MAP = {
    "acquisition": "acquisition/v1/default_acquisition",
    "parse": "parse/v1/default_parse",
    "enrichment": "enrichment/v1/default_enrichment",
}


class LegacyPipelineParser(ParserInterface):
    """Parser for pipeline/v1 (legacy) workflow manifests.

    Translates existing YAML manifests (apiVersion: pipeline/v1)
    into the Model_D parser interface so they can be executed
    by the StandardInterpreter.
    """

    INTERPRETER_PATH = "mda.interpreter.standard_interpreter"

    def __init__(self, manifest_path: str) -> None:
        """Initialize with path to a pipeline/v1 manifest.

        Args:
            manifest_path: Path to the workflow YAML file.
        """
        self.manifest_path = manifest_path
        self._content: dict[str, Any] = {}
        self._steps: list[dict[str, Any]] = []

    def parse(self) -> None:
        """Parse and validate the pipeline/v1 manifest."""
        with open(self.manifest_path, "r") as f:
            data = yaml.safe_load(f)

        # Handle list format (take first item) or single doc
        if isinstance(data, list):
            data = data[0]

        self._content = data

        # Extract steps from spec
        spec = data.get("spec", {})
        self._steps = spec.get("steps", [])

    def get_manifest_id(self) -> str:
        """Get workflow name as manifest ID."""
        return self._content.get("metadata", {}).get("name", "unknown")

    def get_manifest_version(self) -> str:
        """Legacy manifests are always 1.0.0."""
        return "1.0.0"

    def get_provider(self) -> str:
        """Legacy manifests use the built-in ingestion provider."""
        return "mda_ingestion_provider"

    def get_engine(self) -> str:
        """Legacy manifests use the python_v0 engine."""
        return "python_v0"

    def get_steps(self) -> list[dict[str, Any]]:
        """Get step definitions from the workflow spec."""
        return self._steps

    def get_step_name(self, step: dict[str, Any]) -> str:
        """Extract step name."""
        return step.get("name", "unnamed")

    def get_step_component_path(self, step: dict[str, Any]) -> str:
        """Map step type to component path.

        Maps:
            acquisition -> acquisition/v1/default_acquisition
            parse       -> parse/v1/default_parse
            enrichment  -> enrichment/v1/default_enrichment
        """
        step_type = step.get("type", "")
        path = _STEP_TYPE_MAP.get(step_type)
        if not path:
            raise ValueError(
                f"Unknown step type '{step_type}'. "
                f"Known types: {list(_STEP_TYPE_MAP.keys())}"
            )
        return path

    def get_step_component_params(self, step: dict[str, Any]) -> dict[str, Any]:
        """Extract step params, injecting step_name for the adapter."""
        params = dict(step.get("config", {}))
        params["step_name"] = step.get("name", "unnamed")
        return params
