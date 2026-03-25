"""Tests for manifest sync."""

import pytest
import tempfile
from pathlib import Path

from sqlalchemy import create_engine

from db.models import Base
from db.sync import ManifestSync
from db.repository import AgencyRepository, AssetRepository, WorkflowRepository
from db.database import get_session


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
def engine():
    """Create an in-memory SQLite database for testing."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)
    return engine


class TestManifestSync:
    """Tests for ManifestSync."""

    def test_sync_all_manifests(self, temp_manifests, engine):
        """Test syncing all manifests successfully."""
        sync = ManifestSync(temp_manifests, engine)
        report = sync.sync()

        assert report.agencies_synced == 1
        assert report.assets_synced == 1
        assert report.workflows_synced == 1
        assert report.total_errors == 0
        assert report.success is True

    def test_sync_creates_database_entries(self, temp_manifests, engine):
        """Test that sync creates proper database entries."""
        sync = ManifestSync(temp_manifests, engine)
        sync.sync()

        with get_session(engine) as session:
            agency_repo = AgencyRepository(session)
            asset_repo = AssetRepository(session)
            workflow_repo = WorkflowRepository(session)

            agency = agency_repo.get_by_name("test-agency")
            assert agency is not None
            assert agency.full_name == "Test Agency"

            asset = asset_repo.get_by_name("test-asset")
            assert asset is not None
            assert asset.agency_id == agency.id

            workflow = workflow_repo.get_by_name("test-workflow")
            assert workflow is not None
            assert workflow.asset_id == asset.id
            assert len(workflow.steps) == 2

    def test_sync_handles_invalid_reference(self, temp_manifests, engine):
        """Test that sync handles invalid references gracefully."""
        # Create asset with invalid agency reference
        (temp_manifests / "assets" / "bad.yaml").write_text("""
apiVersion: pipeline/v1
kind: Asset
metadata:
  name: bad-asset
spec:
  agencyRef: non-existent-agency
  description: Asset with bad reference
  acquisition:
    type: http
    source:
      url: https://test.gov/data.csv
    format: csv
""")

        sync = ManifestSync(temp_manifests, engine)
        report = sync.sync()

        assert report.assets_synced == 1  # Original asset synced
        assert report.assets_errors == 1  # Bad asset failed
        assert len(report.errors) == 1
        assert "non-existent-agency" in report.errors[0].error

    def test_sync_is_idempotent(self, temp_manifests, engine):
        """Test that running sync twice produces same result."""
        sync = ManifestSync(temp_manifests, engine)

        # First sync
        report1 = sync.sync()
        assert report1.agencies_synced == 1

        # Second sync (should update, not create duplicates)
        report2 = sync.sync()
        assert report2.agencies_synced == 1
        assert report2.total_errors == 0

        # Verify only one agency exists
        with get_session(engine) as session:
            agency_repo = AgencyRepository(session)
            agencies = agency_repo.list_all()
            assert len(agencies) == 1


class TestSyncReport:
    """Tests for SyncReport."""

    def test_total_synced(self, temp_manifests, engine):
        """Test total_synced property."""
        sync = ManifestSync(temp_manifests, engine)
        report = sync.sync()

        assert report.total_synced == 3  # 1 agency + 1 asset + 1 workflow

    def test_success_property(self, temp_manifests, engine):
        """Test success property."""
        sync = ManifestSync(temp_manifests, engine)
        report = sync.sync()

        assert report.success is True
