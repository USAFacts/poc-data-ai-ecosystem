"""Standard Parser v1 — parser for standard/1.0 schema manifests.

Ported from model_D/mda/manifest/parser/v1/standard_parser.py.
"""

from typing import Any

from pydantic import BaseModel

from mda.manifest.parser_interface import ParserInterface


# --- Pydantic Schema Models ---


class Identity(BaseModel):
    """Manifest identity block."""

    name: str
    layer: str
    domain: str
    agency: str
    owner: str = "DAM"


class Evolution(BaseModel):
    """Manifest evolution/versioning block."""

    manifest_id: str
    manifest_version: str
    provider: str
    engine: str


class Component(BaseModel):
    """Step component definition."""

    path: str
    params: dict[str, Any] = {}


class Step(BaseModel):
    """Step definition in a standard manifest."""

    step: str
    component: Component


class StandardManifestSchema(BaseModel):
    """Pydantic model for standard manifest v1 schema validation."""

    identity: Identity
    evolution: Evolution
    steps: list[Step]

    class Config:
        populate_by_name = True
        extra = "allow"  # Allow extra fields like 'schema'


# Schema-Interpreter affinity: standard parser uses standard interpreter
INTERPRETER_PATH = "mda.interpreter.standard_interpreter"


class StandardParser(ParserInterface):
    """Parser for standard/1.0 manifests.

    Standard manifests have:
    - identity: name, layer, domain, agency, owner
    - evolution: manifest_id, manifest_version, provider, engine
    - steps: list of {step, component: {path, params}}
    """

    INTERPRETER_PATH = "mda.interpreter.standard_interpreter"

    def __init__(self, manifest_content: str | dict) -> None:
        """Initialize with manifest content (dict or file path).

        Args:
            manifest_content: Parsed manifest dict or file path string.
        """
        if isinstance(manifest_content, str):
            # If it's a file path, load it
            import yaml

            with open(manifest_content, "r") as f:
                self.manifest_content = yaml.safe_load(f)
        else:
            self.manifest_content = manifest_content
        self._manifest: StandardManifestSchema | None = None

    def parse(self) -> None:
        """Parse and validate manifest content using Pydantic."""
        self._manifest = StandardManifestSchema(**self.manifest_content)

    def get_manifest_id(self) -> str:
        return self._manifest.evolution.manifest_id

    def get_manifest_version(self) -> str:
        return self._manifest.evolution.manifest_version

    def get_provider(self) -> str:
        return self._manifest.evolution.provider

    def get_engine(self) -> str:
        return self._manifest.evolution.engine

    def get_steps(self) -> list[dict[str, Any]]:
        return [step.model_dump() for step in self._manifest.steps]

    def get_step_name(self, step: dict[str, Any]) -> str:
        return step["step"]

    def get_step_component_path(self, step: dict[str, Any]) -> str:
        return step["component"]["path"]

    def get_step_component_params(self, step: dict[str, Any]) -> dict[str, Any]:
        params = dict(step["component"].get("params", {}))
        # Inject step_name for the CapabilityAdapter
        if "step_name" not in params:
            params["step_name"] = step["step"]
        return params

    # --- Extra helpers for DB sync ---

    @property
    def identity(self) -> Identity:
        """Access parsed identity block."""
        return self._manifest.identity

    @property
    def evolution(self) -> Evolution:
        """Access parsed evolution block."""
        return self._manifest.evolution
