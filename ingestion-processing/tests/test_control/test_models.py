"""Tests for control plane models."""

import pytest
from pydantic import ValidationError

from pipeline.control.models import (
    AcquisitionConfig,
    AcquisitionType,
    Agency,
    AgencySpec,
    Asset,
    AssetSpec,
    HttpSource,
    Metadata,
    StepConfig,
    Workflow,
    WorkflowSpec,
)


class TestAgency:
    """Tests for Agency model."""

    def test_valid_agency(self) -> None:
        """Test creating a valid agency."""
        agency = Agency(
            apiVersion="pipeline/v1",
            kind="Agency",
            metadata=Metadata(name="test-agency", labels={"category": "test"}),
            spec=AgencySpec(
                fullName="Test Agency",
                baseUrl="https://test.gov",
                description="A test agency",
            ),
        )

        assert agency.metadata.name == "test-agency"
        assert agency.spec.full_name == "Test Agency"
        assert agency.spec.base_url == "https://test.gov"

    def test_agency_from_dict(self) -> None:
        """Test creating agency from dictionary (as from YAML)."""
        data = {
            "apiVersion": "pipeline/v1",
            "kind": "Agency",
            "metadata": {"name": "test-agency", "labels": {}},
            "spec": {
                "fullName": "Test Agency",
                "baseUrl": "https://test.gov",
                "description": "",
            },
        }

        agency = Agency.model_validate(data)
        assert agency.metadata.name == "test-agency"


class TestAsset:
    """Tests for Asset model."""

    def test_valid_asset_http(self) -> None:
        """Test creating a valid HTTP asset."""
        asset = Asset(
            apiVersion="pipeline/v1",
            kind="Asset",
            metadata=Metadata(name="test-asset", labels={"domain": "test"}),
            spec=AssetSpec(
                agencyRef="test-agency",
                description="A test asset",
                acquisition=AcquisitionConfig(
                    type=AcquisitionType.HTTP,
                    source=HttpSource(url="https://test.gov/data.csv"),
                    format="csv",
                ),
            ),
        )

        assert asset.metadata.name == "test-asset"
        assert asset.spec.agency_ref == "test-agency"
        assert asset.spec.acquisition.type == AcquisitionType.HTTP

    def test_asset_from_dict(self) -> None:
        """Test creating asset from dictionary."""
        data = {
            "apiVersion": "pipeline/v1",
            "kind": "Asset",
            "metadata": {"name": "test-asset", "labels": {}},
            "spec": {
                "agencyRef": "test-agency",
                "description": "",
                "acquisition": {
                    "type": "http",
                    "source": {"url": "https://test.gov/data.csv"},
                    "format": "csv",
                },
            },
        }

        asset = Asset.model_validate(data)
        assert asset.spec.acquisition.type == AcquisitionType.HTTP


class TestWorkflow:
    """Tests for Workflow model."""

    def test_valid_workflow(self) -> None:
        """Test creating a valid workflow."""
        workflow = Workflow(
            apiVersion="pipeline/v1",
            kind="Workflow",
            metadata=Metadata(name="test-workflow", labels={}),
            spec=WorkflowSpec(
                assetRef="test-asset",
                steps=[
                    StepConfig(name="acquire", type="acquisition", config={}),
                ],
            ),
        )

        assert workflow.metadata.name == "test-workflow"
        assert workflow.spec.asset_ref == "test-asset"
        assert len(workflow.spec.steps) == 1

    def test_get_step(self) -> None:
        """Test getting a step by name."""
        workflow = Workflow(
            apiVersion="pipeline/v1",
            kind="Workflow",
            metadata=Metadata(name="test-workflow", labels={}),
            spec=WorkflowSpec(
                assetRef="test-asset",
                steps=[
                    StepConfig(name="acquire", type="acquisition", config={}),
                    StepConfig(name="validate", type="validation", config={}),
                ],
            ),
        )

        step = workflow.get_step("acquire")
        assert step is not None
        assert step.name == "acquire"

        missing = workflow.get_step("nonexistent")
        assert missing is None
