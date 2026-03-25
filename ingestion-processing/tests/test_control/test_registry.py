"""Tests for registry loader."""

from pathlib import Path

import pytest

from pipeline.control.registry import Registry, RegistryError


class TestRegistry:
    """Tests for Registry class."""

    def test_load_valid_manifests(
        self,
        manifests_path: Path,
        sample_agency_yaml: str,
        sample_asset_yaml: str,
        sample_workflow_yaml: str,
    ) -> None:
        """Test loading valid manifests."""
        # Write test files
        (manifests_path / "agencies" / "test.yaml").write_text(sample_agency_yaml)
        (manifests_path / "assets" / "test.yaml").write_text(sample_asset_yaml)
        (manifests_path / "workflows" / "test.yaml").write_text(sample_workflow_yaml)

        registry = Registry(manifests_path)
        registry.load()

        assert "test-agency" in registry.agencies
        assert "test-asset" in registry.assets
        assert "test-workflow" in registry.workflows

    def test_invalid_agency_reference(
        self,
        manifests_path: Path,
        sample_asset_yaml: str,
    ) -> None:
        """Test that invalid agency reference raises error."""
        (manifests_path / "assets" / "test.yaml").write_text(sample_asset_yaml)

        registry = Registry(manifests_path)
        with pytest.raises(RegistryError, match="unknown agency"):
            registry.load()

    def test_duplicate_names(
        self,
        manifests_path: Path,
        sample_agency_yaml: str,
    ) -> None:
        """Test that duplicate names raise error."""
        (manifests_path / "agencies" / "test1.yaml").write_text(sample_agency_yaml)
        (manifests_path / "agencies" / "test2.yaml").write_text(sample_agency_yaml)

        registry = Registry(manifests_path)
        with pytest.raises(RegistryError, match="Duplicate agency"):
            registry.load()

    def test_get_workflow_chain(
        self,
        manifests_path: Path,
        sample_agency_yaml: str,
        sample_asset_yaml: str,
        sample_workflow_yaml: str,
    ) -> None:
        """Test getting workflow -> asset -> agency chain."""
        (manifests_path / "agencies" / "test.yaml").write_text(sample_agency_yaml)
        (manifests_path / "assets" / "test.yaml").write_text(sample_asset_yaml)
        (manifests_path / "workflows" / "test.yaml").write_text(sample_workflow_yaml)

        registry = Registry(manifests_path)
        registry.load()

        workflow = registry.get_workflow("test-workflow")
        asset = registry.get_workflow_asset(workflow)
        agency = registry.get_workflow_agency(workflow)

        assert workflow.metadata.name == "test-workflow"
        assert asset.metadata.name == "test-asset"
        assert agency.metadata.name == "test-agency"

    def test_not_loaded_error(self, manifests_path: Path) -> None:
        """Test accessing registry before loading raises error."""
        registry = Registry(manifests_path)

        with pytest.raises(RegistryError, match="not loaded"):
            _ = registry.agencies
