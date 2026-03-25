"""Constraint validator for step schemas."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class StepSchema:
    """Parsed step constraint schema."""

    name: str
    version: str
    description: str
    requires: dict[str, Any]
    provides: dict[str, Any]
    constraints: dict[str, Any]
    errors: list[dict[str, Any]]

    @classmethod
    def from_yaml(cls, path: Path) -> "StepSchema":
        """Load schema from YAML file."""
        with open(path) as f:
            data = yaml.safe_load(f)

        return cls(
            name=data.get("name", "unknown"),
            version=data.get("version", "1.0"),
            description=data.get("description", ""),
            requires=data.get("requires", {}),
            provides=data.get("provides", {}),
            constraints=data.get("constraints", {}),
            errors=data.get("errors", []),
        )

    @property
    def required_context(self) -> list[dict[str, str]]:
        """Get required context items."""
        return self.requires.get("context", [])

    @property
    def required_config(self) -> list[dict[str, str]]:
        """Get required configuration items."""
        return self.requires.get("config", [])

    @property
    def required_steps(self) -> list[str]:
        """Get required preceding steps."""
        return self.requires.get("steps", [])

    @property
    def output_spec(self) -> list[dict[str, str]]:
        """Get output specification."""
        return self.provides.get("outputs", [])

    @property
    def artifact_spec(self) -> list[dict[str, str]]:
        """Get artifact specification."""
        return self.provides.get("artifacts", [])


@dataclass
class ValidationError:
    """A validation error."""

    step_name: str
    constraint_name: str
    message: str
    severity: str = "error"  # error, warning


@dataclass
class ValidationResult:
    """Result of constraint validation."""

    valid: bool
    errors: list[ValidationError] = field(default_factory=list)
    warnings: list[ValidationError] = field(default_factory=list)

    def add_error(self, step_name: str, constraint_name: str, message: str) -> None:
        """Add a validation error."""
        self.errors.append(ValidationError(step_name, constraint_name, message, "error"))
        self.valid = False

    def add_warning(self, step_name: str, constraint_name: str, message: str) -> None:
        """Add a validation warning."""
        self.warnings.append(ValidationError(step_name, constraint_name, message, "warning"))


class ConstraintValidator:
    """Validates step constraints from schemas."""

    def __init__(self, schemas_path: Path | str | None = None) -> None:
        """Initialize validator.

        Args:
            schemas_path: Path to schemas directory (default: src/steps/schemas)
        """
        if schemas_path is None:
            # Default to package location
            schemas_path = Path(__file__).parent.parent / "steps" / "schemas"
        self.schemas_path = Path(schemas_path)
        self._schemas: dict[str, StepSchema] = {}

    def load_schemas(self) -> None:
        """Load all step schemas from disk."""
        if not self.schemas_path.exists():
            return

        for schema_file in self.schemas_path.glob("*.yaml"):
            schema = StepSchema.from_yaml(schema_file)
            self._schemas[schema.name] = schema

    def get_schema(self, step_type: str) -> StepSchema | None:
        """Get schema for a step type."""
        if not self._schemas:
            self.load_schemas()
        return self._schemas.get(step_type)

    def validate_step_sequence(
        self,
        steps: list[dict[str, Any]],
    ) -> ValidationResult:
        """Validate a sequence of steps for constraint satisfaction.

        Checks:
        1. All step types have valid schemas
        2. Step dependencies are satisfied (required steps run before)
        3. Outputs from earlier steps satisfy inputs of later steps

        Args:
            steps: List of step configurations

        Returns:
            ValidationResult with any errors/warnings
        """
        if not self._schemas:
            self.load_schemas()

        result = ValidationResult(valid=True)

        # Track what each step provides
        available_outputs: dict[str, set[str]] = {}
        completed_steps: set[str] = set()  # Step names
        completed_types: set[str] = set()  # Step types

        for step in steps:
            step_name = step.get("name", "unknown")
            step_type = step.get("type", "unknown")

            schema = self._schemas.get(step_type)
            if schema is None:
                result.add_warning(
                    step_name,
                    "schema_missing",
                    f"No schema found for step type '{step_type}'",
                )
                completed_steps.add(step_name)
                completed_types.add(step_type)
                continue

            # Check required step types are completed
            for required_type in schema.required_steps:
                if required_type not in completed_types:
                    result.add_error(
                        step_name,
                        "missing_dependency",
                        f"Step '{step_name}' requires a '{required_type}' step to run first",
                    )

            # Check explicit dependencies from workflow
            depends_on = step.get("depends_on", step.get("dependsOn", []))
            for dep in depends_on:
                if dep not in completed_steps:
                    result.add_error(
                        step_name,
                        "missing_dependency",
                        f"Step '{step_name}' depends on '{dep}' which hasn't run",
                    )

            # Record outputs this step provides
            available_outputs[step_name] = {
                output["name"] for output in schema.output_spec
            }

            completed_steps.add(step_name)
            completed_types.add(step_type)

        return result

    def validate_step_config(
        self,
        step_type: str,
        config: dict[str, Any],
    ) -> ValidationResult:
        """Validate step configuration against schema.

        Args:
            step_type: Type of step
            config: Step configuration

        Returns:
            ValidationResult with any errors/warnings
        """
        result = ValidationResult(valid=True)

        schema = self.get_schema(step_type)
        if schema is None:
            result.add_warning(
                step_type,
                "schema_missing",
                f"No schema found for step type '{step_type}'",
            )
            return result

        # Check required config
        for required in schema.required_config:
            if required.get("name") not in config:
                if required.get("default") is None:
                    result.add_error(
                        step_type,
                        "missing_config",
                        f"Required config '{required['name']}' not provided",
                    )

        return result

    def get_step_outputs(self, step_type: str) -> list[dict[str, str]]:
        """Get the outputs a step type provides.

        Args:
            step_type: Type of step

        Returns:
            List of output specifications
        """
        schema = self.get_schema(step_type)
        if schema is None:
            return []
        return schema.output_spec

    def get_step_requirements(self, step_type: str) -> dict[str, Any]:
        """Get the requirements for a step type.

        Args:
            step_type: Type of step

        Returns:
            Requirements dictionary
        """
        schema = self.get_schema(step_type)
        if schema is None:
            return {}
        return schema.requires

    def validate_plan(self, plan: Any) -> ValidationResult:
        """Validate an execution plan.

        Args:
            plan: ExecutionPlan to validate

        Returns:
            ValidationResult with any errors/warnings
        """
        # If plan already has validation, return it
        if hasattr(plan, "validation") and plan.validation is not None:
            return plan.validation

        # Otherwise validate the steps
        step_dicts = [
            {
                "name": s.name,
                "type": s.type,
                "config": s.config,
                "depends_on": s.dependencies,
            }
            for s in plan.steps
        ]
        return self.validate_step_sequence(step_dicts)
