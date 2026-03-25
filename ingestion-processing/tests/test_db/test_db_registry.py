"""Tests for DbRegistry."""

import pytest
import tempfile
from pathlib import Path

from sqlalchemy import create_engine

from db.models import Base
from db.sync import ManifestSync
from db.db_registry import DbRegistry
from control.compiler import Compiler


@pytest.fixture
def temp_manifests():
    """Create temporary manifest files for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmppath = Path(tmpdir)

        # Create directories
        (tmppath / "agencies").mkdir()
        (tmppath / "assets").mkdir()
        (tmppath / "workflows").mkdir()

        # Create agency manifest
        (tmppath / "agencies" / "test.yaml").write_text("""
apiVersion: pipeline/v1
kind: Agency
metadata:
  name: test-agency
  labels:
    category: test
spec:
  fullName: Test Agency
  baseUrl: https://test.gov
  description: A test agency
""")

        # Create asset manifest
        (tmppath / "assets" / "test.yaml").write_text("""
apiVersion: pipeline/v1
kind: Asset
metadata:
  name: test-asset
  labels:
    format: csv
spec:
  agencyRef: test-agency
  description: A test asset
  acquisition:
    type: http
    source:
      url: https://test.gov/data.csv
    format: csv
""")

        # Create workflow manifest
        (tmppath / "workflows" / "test.yaml").write_text("""
apiVersion: pipeline/v1
kind: Workflow
metadata:
  name: test-workflow
spec:
  assetRef: test-asset
  steps:
    - name: acquire
      type: acquisition
      config: {}
    - name: parse
      type: parse
      dependsOn:
        - acquire
      config:
        parser: basic
""")

        yield tmppath


@pytest.fixture
def populated_engine(temp_manifests):
    """Create and populate an in-memory database."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)

    # Sync manifests
    sync = ManifestSync(temp_manifests, engine)
    sync.sync()

    return engine


class TestDbRegistry:
    """Tests for DbRegistry."""

    def test_load_agencies(self, populated_engine):
        """Test loading agencies from database."""
        registry = DbRegistry(populated_engine)
        registry.load()

        assert len(registry.agencies) == 1
        agency = registry.get_agency("test-agency")
        assert agency.spec.full_name == "Test Agency"

    def test_load_assets(self, populated_engine):
        """Test loading assets from database."""
        registry = DbRegistry(populated_engine)
        registry.load()

        assert len(registry.assets) == 1
        asset = registry.get_asset("test-asset")
        assert asset.spec.agency_ref == "test-agency"

    def test_load_workflows(self, populated_engine):
        """Test loading workflows from database."""
        registry = DbRegistry(populated_engine)
        registry.load()

        assert len(registry.workflows) == 1
        workflow = registry.get_workflow("test-workflow")
        assert workflow.spec.asset_ref == "test-asset"
        assert len(workflow.spec.steps) == 2

    def test_get_asset_agency(self, populated_engine):
        """Test navigating from asset to agency."""
        registry = DbRegistry(populated_engine)
        registry.load()

        asset = registry.get_asset("test-asset")
        agency = registry.get_asset_agency(asset)
        assert agency.metadata.name == "test-agency"

    def test_get_workflow_asset(self, populated_engine):
        """Test navigating from workflow to asset."""
        registry = DbRegistry(populated_engine)
        registry.load()

        workflow = registry.get_workflow("test-workflow")
        asset = registry.get_workflow_asset(workflow)
        assert asset.metadata.name == "test-asset"

    def test_get_workflow_agency(self, populated_engine):
        """Test navigating from workflow to agency."""
        registry = DbRegistry(populated_engine)
        registry.load()

        workflow = registry.get_workflow("test-workflow")
        agency = registry.get_workflow_agency(workflow)
        assert agency.metadata.name == "test-agency"

    def test_works_with_compiler(self, populated_engine):
        """Test that DbRegistry works with the Compiler."""
        registry = DbRegistry(populated_engine)
        registry.load()

        compiler = Compiler(registry)
        plan = compiler.compile("test-workflow")

        assert plan.workflow_name == "test-workflow"
        assert plan.asset.metadata.name == "test-asset"
        assert plan.agency.metadata.name == "test-agency"
        assert len(plan.steps) == 2
        assert plan.execution_order == ["acquire", "parse"]

    def test_raises_error_when_not_loaded(self, populated_engine):
        """Test that accessing properties before load raises error."""
        registry = DbRegistry(populated_engine)

        from control.registry import RegistryError

        with pytest.raises(RegistryError, match="not loaded"):
            _ = registry.agencies

    def test_raises_error_for_unknown_entity(self, populated_engine):
        """Test that getting unknown entities raises error."""
        registry = DbRegistry(populated_engine)
        registry.load()

        from control.registry import RegistryError

        with pytest.raises(RegistryError, match="not found"):
            registry.get_agency("non-existent")
