"""Tests for DAG builder."""

import pytest

from pipeline.control.models import (
    AcquisitionConfig,
    AcquisitionType,
    Asset,
    AssetSpec,
    HttpSource,
    Metadata,
    StepConfig,
    Workflow,
    WorkflowSpec,
)
from pipeline.execution.dag import DAG, DAGError


def make_workflow(steps: list[StepConfig]) -> Workflow:
    """Helper to create workflow with steps."""
    return Workflow(
        apiVersion="pipeline/v1",
        kind="Workflow",
        metadata=Metadata(name="test-workflow", labels={}),
        spec=WorkflowSpec(assetRef="test-asset", steps=steps),
    )


class TestDAG:
    """Tests for DAG class."""

    def test_simple_dag(self) -> None:
        """Test creating a simple single-step DAG."""
        workflow = make_workflow(
            [StepConfig(name="acquire", type="acquisition", config={})]
        )

        dag = DAG.from_workflow(workflow)

        assert len(dag) == 1
        assert "acquire" in dag.nodes

    def test_dag_with_dependencies(self) -> None:
        """Test DAG with step dependencies."""
        workflow = make_workflow(
            [
                StepConfig(name="acquire", type="acquisition", config={}),
                StepConfig(
                    name="validate",
                    type="acquisition",  # Using acquisition as placeholder
                    config={},
                    dependsOn=["acquire"],
                ),
            ]
        )

        dag = DAG.from_workflow(workflow)
        order = dag.topological_sort()

        assert order.index("acquire") < order.index("validate")

    def test_topological_sort(self) -> None:
        """Test topological sorting with multiple dependencies."""
        workflow = make_workflow(
            [
                StepConfig(name="step1", type="acquisition", config={}),
                StepConfig(name="step2", type="acquisition", config={}, dependsOn=["step1"]),
                StepConfig(name="step3", type="acquisition", config={}, dependsOn=["step1"]),
                StepConfig(
                    name="step4", type="acquisition", config={}, dependsOn=["step2", "step3"]
                ),
            ]
        )

        dag = DAG.from_workflow(workflow)
        order = dag.topological_sort()

        # step1 must come before step2 and step3
        assert order.index("step1") < order.index("step2")
        assert order.index("step1") < order.index("step3")
        # step4 must come after step2 and step3
        assert order.index("step2") < order.index("step4")
        assert order.index("step3") < order.index("step4")

    def test_unknown_step_type(self) -> None:
        """Test that unknown step type raises error."""
        workflow = make_workflow(
            [StepConfig(name="unknown", type="nonexistent_type", config={})]
        )

        with pytest.raises(DAGError, match="Unknown step type"):
            DAG.from_workflow(workflow)

    def test_missing_dependency(self) -> None:
        """Test that missing dependency raises error."""
        workflow = make_workflow(
            [
                StepConfig(
                    name="step1", type="acquisition", config={}, dependsOn=["nonexistent"]
                ),
            ]
        )

        with pytest.raises(DAGError, match="unknown step"):
            DAG.from_workflow(workflow)

    def test_circular_dependency(self) -> None:
        """Test that circular dependency raises error."""
        workflow = make_workflow(
            [
                StepConfig(name="step1", type="acquisition", config={}, dependsOn=["step2"]),
                StepConfig(name="step2", type="acquisition", config={}, dependsOn=["step1"]),
            ]
        )

        with pytest.raises(DAGError, match="Circular"):
            DAG.from_workflow(workflow)
