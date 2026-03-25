"""Tests for database models."""

import pytest
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from db.models import Base, AgencyModel, AssetModel, WorkflowModel, SyncLogModel


@pytest.fixture
def engine():
    """Create an in-memory SQLite database for testing."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)
    return engine


@pytest.fixture
def session(engine):
    """Create a session for testing."""
    with Session(engine) as session:
        yield session


class TestAgencyModel:
    """Tests for AgencyModel."""

    def test_create_agency(self, session):
        """Test creating an agency."""
        agency = AgencyModel(
            name="test-agency",
            full_name="Test Agency",
            base_url="https://test.gov",
            description="A test agency",
            labels={"category": "test"},
        )
        session.add(agency)
        session.commit()

        assert agency.id is not None
        assert agency.name == "test-agency"
        assert agency.full_name == "Test Agency"
        assert agency.created_at is not None

    def test_agency_unique_name(self, session):
        """Test that agency names must be unique."""
        agency1 = AgencyModel(name="unique", full_name="Agency 1")
        session.add(agency1)
        session.commit()

        agency2 = AgencyModel(name="unique", full_name="Agency 2")
        session.add(agency2)

        with pytest.raises(Exception):  # IntegrityError
            session.commit()


class TestAssetModel:
    """Tests for AssetModel."""

    def test_create_asset_with_agency(self, session):
        """Test creating an asset linked to an agency."""
        agency = AgencyModel(name="agency", full_name="Agency")
        session.add(agency)
        session.flush()

        asset = AssetModel(
            name="test-asset",
            agency_id=agency.id,
            description="A test asset",
            acquisition_config={"type": "http", "format": "csv"},
        )
        session.add(asset)
        session.commit()

        assert asset.id is not None
        assert asset.agency_id == agency.id
        assert asset.agency == agency

    def test_asset_cascade_delete(self, session):
        """Test that deleting an agency cascades to assets."""
        agency = AgencyModel(name="agency", full_name="Agency")
        session.add(agency)
        session.flush()

        asset = AssetModel(
            name="asset",
            agency_id=agency.id,
            acquisition_config={"type": "http"},
        )
        session.add(asset)
        session.commit()

        # Delete agency should cascade to asset
        session.delete(agency)
        session.commit()

        assert session.query(AssetModel).count() == 0


class TestWorkflowModel:
    """Tests for WorkflowModel."""

    def test_create_workflow(self, session):
        """Test creating a workflow linked to an asset."""
        agency = AgencyModel(name="agency", full_name="Agency")
        session.add(agency)
        session.flush()

        asset = AssetModel(
            name="asset",
            agency_id=agency.id,
            acquisition_config={"type": "http"},
        )
        session.add(asset)
        session.flush()

        workflow = WorkflowModel(
            name="test-workflow",
            asset_id=asset.id,
            steps=[
                {"name": "acquire", "type": "acquisition", "config": {}},
                {"name": "parse", "type": "parse", "dependsOn": ["acquire"]},
            ],
        )
        session.add(workflow)
        session.commit()

        assert workflow.id is not None
        assert workflow.asset_id == asset.id
        assert len(workflow.steps) == 2


class TestSyncLogModel:
    """Tests for SyncLogModel."""

    def test_create_sync_log(self, session):
        """Test creating a sync log entry."""
        log = SyncLogModel(
            entity_type="agency",
            entity_name="test-agency",
            status="success",
        )
        session.add(log)
        session.commit()

        assert log.id is not None
        assert log.synced_at is not None

    def test_sync_log_with_error(self, session):
        """Test creating a sync log with an error."""
        log = SyncLogModel(
            entity_type="asset",
            entity_name="test-asset",
            status="error",
            error_message="Failed to sync: invalid reference",
        )
        session.add(log)
        session.commit()

        assert log.error_message == "Failed to sync: invalid reference"
