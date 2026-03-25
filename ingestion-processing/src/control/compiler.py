"""Compiler that transforms manifests into validated execution plans."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol

from control.models import Agency, Asset, Workflow, StepConfig
from control.registry import Registry
from control.validator import ConstraintValidator, ValidationResult


class RegistryProtocol(Protocol):
    """Protocol defining the interface for a registry.

    Both Registry and DbRegistry implement this interface.
    """

    @property
    def agencies(self) -> dict[str, Agency]: ...
    @property
    def assets(self) -> dict[str, Asset]: ...
    @property
    def workflows(self) -> dict[str, Workflow]: ...

    def get_agency(self, name: str) -> Agency: ...
    def get_asset(self, name: str) -> Asset: ...
    def get_workflow(self, name: str) -> Workflow: ...
    def get_asset_agency(self, asset: Asset) -> Agency: ...
    def get_workflow_asset(self, workflow: Workflow) -> Asset: ...
    def get_workflow_agency(self, workflow: Workflow) -> Agency: ...


class CompilationError(Exception):
    """Error during manifest compilation."""

    pass


@dataclass
class ExecutionStep:
    """A compiled step ready for execution."""

    name: str
    type: str
    config: dict[str, Any]
    dependencies: list[str]

    # Resolved from schema
    provides: list[str] = field(default_factory=list)
    requires: list[str] = field(default_factory=list)


@dataclass
class ExecutionPlan:
    """A validated, compiled execution plan.

    Produced by the Compiler from workflow manifests.
    Consumed by the Runtime executor.
    """

    workflow_name: str
    asset: Asset
    agency: Agency
    steps: list[ExecutionStep]
    execution_order: list[str]

    # Compilation metadata
    compiled_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    plan_id: str = ""

    # Validation results
    validation: ValidationResult | None = None

    def __post_init__(self) -> None:
        """Generate plan ID if not provided."""
        if not self.plan_id:
            self.plan_id = self.compiled_at.strftime("%Y%m%d_%H%M%S")

    @property
    def is_valid(self) -> bool:
        """Check if plan passed validation."""
        return self.validation is None or self.validation.valid

    def get_step(self, name: str) -> ExecutionStep | None:
        """Get a step by name."""
        for step in self.steps:
            if step.name == name:
                return step
        return None

    def to_dict(self) -> dict[str, Any]:
        """Serialize plan to dictionary."""
        return {
            "workflow_name": self.workflow_name,
            "asset": self.asset.metadata.name,
            "agency": self.agency.metadata.name,
            "plan_id": self.plan_id,
            "compiled_at": self.compiled_at.isoformat(),
            "execution_order": self.execution_order,
            "steps": [
                {
                    "name": s.name,
                    "type": s.type,
                    "config": s.config,
                    "dependencies": s.dependencies,
                    "provides": s.provides,
                    "requires": s.requires,
                }
                for s in self.steps
            ],
            "valid": self.is_valid,
        }


class Compiler:
    """Compiles workflow manifests into executable plans.

    The compiler:
    1. Resolves all references (workflow -> asset -> agency)
    2. Validates step constraints using schemas
    3. Computes execution order via topological sort
    4. Produces an ExecutionPlan for the runtime
    """

    def __init__(
        self,
        registry: RegistryProtocol,
        validator: ConstraintValidator | None = None,
    ) -> None:
        """Initialize compiler.

        Args:
            registry: Loaded manifest registry (Registry or DbRegistry)
            validator: Constraint validator (default: create new)
        """
        self.registry = registry
        self.validator = validator or ConstraintValidator()
        self.validator.load_schemas()

    def compile(self, workflow_name: str) -> ExecutionPlan:
        """Compile a workflow into an execution plan.

        Args:
            workflow_name: Name of workflow to compile

        Returns:
            Validated ExecutionPlan

        Raises:
            CompilationError: If compilation fails
        """
        # Resolve references
        workflow = self.registry.get_workflow(workflow_name)
        asset = self.registry.get_workflow_asset(workflow)
        agency = self.registry.get_workflow_agency(workflow)

        # Compile steps
        steps = self._compile_steps(workflow)

        # Compute execution order
        execution_order = self._topological_sort(steps)

        # Validate constraints
        validation = self._validate(workflow, steps)

        plan = ExecutionPlan(
            workflow_name=workflow_name,
            asset=asset,
            agency=agency,
            steps=steps,
            execution_order=execution_order,
            validation=validation,
        )

        if not plan.is_valid:
            errors = [f"{e.step_name}: {e.message}" for e in validation.errors]
            raise CompilationError(
                f"Workflow '{workflow_name}' failed validation:\n" +
                "\n".join(f"  - {e}" for e in errors)
            )

        return plan

    def compile_all(self) -> list[ExecutionPlan]:
        """Compile all workflows in registry.

        Returns:
            List of ExecutionPlans
        """
        plans = []
        for workflow_name in self.registry.workflows:
            plan = self.compile(workflow_name)
            plans.append(plan)
        return plans

    def _compile_steps(self, workflow: Workflow) -> list[ExecutionStep]:
        """Compile workflow steps into ExecutionSteps."""
        steps = []

        for step_config in workflow.spec.steps:
            # Get schema info for this step type
            schema = self.validator.get_schema(step_config.type)

            provides = []
            requires = []
            if schema:
                provides = [o["name"] for o in schema.output_spec]
                requires = schema.required_steps

            exec_step = ExecutionStep(
                name=step_config.name,
                type=step_config.type,
                config=step_config.config,
                dependencies=list(step_config.depends_on),
                provides=provides,
                requires=requires,
            )
            steps.append(exec_step)

        return steps

    def _topological_sort(self, steps: list[ExecutionStep]) -> list[str]:
        """Compute execution order via topological sort.

        Args:
            steps: List of compiled steps

        Returns:
            List of step names in execution order

        Raises:
            CompilationError: If circular dependency detected
        """
        # Build adjacency info
        step_map = {s.name: s for s in steps}
        in_degree = {s.name: len(s.dependencies) for s in steps}

        # Start with steps that have no dependencies
        queue = [name for name, degree in in_degree.items() if degree == 0]
        result = []

        while queue:
            queue.sort()  # Deterministic order
            current = queue.pop(0)
            result.append(current)

            # Reduce in-degree for dependent steps
            for step in steps:
                if current in step.dependencies:
                    in_degree[step.name] -= 1
                    if in_degree[step.name] == 0:
                        queue.append(step.name)

        if len(result) != len(steps):
            raise CompilationError("Circular dependency detected in workflow")

        return result

    def _validate(
        self,
        workflow: Workflow,
        steps: list[ExecutionStep],
    ) -> ValidationResult:
        """Validate workflow against step constraints.

        Args:
            workflow: Source workflow
            steps: Compiled steps

        Returns:
            ValidationResult
        """
        # Convert to format validator expects
        step_dicts = [
            {
                "name": s.name,
                "type": s.type,
                "config": s.config,
                "depends_on": s.dependencies,
            }
            for s in steps
        ]

        # Validate step sequence
        result = self.validator.validate_step_sequence(step_dicts)

        # Validate individual step configs
        for step in steps:
            config_result = self.validator.validate_step_config(step.type, step.config)
            result.errors.extend(config_result.errors)
            result.warnings.extend(config_result.warnings)
            if not config_result.valid:
                result.valid = False

        return result

    def validate_only(self, workflow_name: str) -> ValidationResult:
        """Validate a workflow without producing a full plan.

        Args:
            workflow_name: Name of workflow to validate

        Returns:
            ValidationResult
        """
        try:
            workflow = self.registry.get_workflow(workflow_name)
            steps = self._compile_steps(workflow)
            self._topological_sort(steps)  # Check for cycles
            return self._validate(workflow, steps)
        except Exception as e:
            result = ValidationResult(valid=False)
            result.add_error(workflow_name, "compilation", str(e))
            return result
