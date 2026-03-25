"""Tests for repository classes."""

import pytest

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from db.models import Base, AgencyModel, AssetModel, WorkflowModel
from db.repository import (
    AgencyRepository,
    AssetRepository,
    WorkflowRepository,
    SyncLogRepository,
)


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


class TestAgencyRepository:
    """Tests for AgencyRepository."""

    def test_create_and_get_by_name(self, session):
        """Test creating and retrieving an agency by name."""
        repo = AgencyRepository(session)

        agency = repo.create(
            name="test-agency",
            full_name="Test Agency",
            base_url="https://test.gov",
        )
        session.commit()

        found = repo.get_by_name("test-agency")
        assert found is not None
        assert found.full_name == "Test Agency"

    def test_upsert_creates_new(self, session):
        """Test upsert creates a new agency."""
        repo = AgencyRepository(session)

        agency, created = repo.upsert(
            name="new-agency",
            full_name="New Agency",
        )
        session.commit()

        assert created is True
        assert agency.name == "new-agency"

    def test_upsert_updates_existing(self, session):
        """Test upsert updates an existing agency."""
        repo = AgencyRepository(session)

        # Create first
        repo.create(name="agency", full_name="Original Name")
        session.commit()

        # Upsert should update
        agency, created = repo.upsert(
            name="agency",
            full_name="Updated Name",
        )
        session.commit()

        assert created is False
        assert agency.full_name == "Updated Name"

    def test_list_all(self, session):
        """Test listing all agencies."""
        repo = AgencyRepository(session)

        repo.create(name="agency-a", full_name="Agency A")
        repo.create(name="agency-b", full_name="Agency B")
        session.commit()

        agencies = repo.list_all()
        assert len(agencies) == 2
        # Should be sorted by name
        assert agencies[0].name == "agency-a"
        assert agencies[1].name == "agency-b"


class TestAssetRepository:
    """Tests for AssetRepository."""

    @pytest.fixture
    def agency_id(self, session):
        """Create an agency and return its ID."""
        agency = AgencyModel(name="agency", full_name="Agency")
        session.add(agency)
        session.flush()
        return agency.id

    def test_create_and_get_by_name(self, session, agency_id):
        """Test creating and retrieving an asset by name."""
        repo = AssetRepository(session)

        asset = repo.create(
            name="test-asset",
            agency_id=agency_id,
            acquisition_config={"type": "http", "format": "csv"},
        )
        session.commit()

        found = repo.get_by_name("test-asset")
        assert found is not None
        assert found.acquisition_config["format"] == "csv"

    def test_list_by_agency(self, session, agency_id):
        """Test listing assets by agency."""
        repo = AssetRepository(session)

        repo.create(name="asset-1", agency_id=agency_id, acquisition_config={})
        repo.create(name="asset-2", agency_id=agency_id, acquisition_config={})
        session.commit()

        assets = repo.list_by_agency(agency_id)
        assert len(assets) == 2


class TestWorkflowRepository:
    """Tests for WorkflowRepository."""

    @pytest.fixture
    def asset_id(self, session):
        """Create an agency and asset, return asset ID."""
        agency = AgencyModel(name="agency", full_name="Agency")
        session.add(agency)
        session.flush()

        asset = AssetModel(
            name="asset",
            agency_id=agency.id,
            acquisition_config={},
        )
        session.add(asset)
        session.flush()
        return asset.id

    def test_create_and_get_by_name(self, session, asset_id):
        """Test creating and retrieving a workflow by name."""
        repo = WorkflowRepository(session)

        workflow = repo.create(
            name="test-workflow",
            asset_id=asset_id,
            steps=[{"name": "acquire", "type": "acquisition"}],
        )
        session.commit()

        found = repo.get_by_name("test-workflow")
        assert found is not None
        assert len(found.steps) == 1

    def test_upsert_workflow(self, session, asset_id):
        """Test upserting a workflow."""
        repo = WorkflowRepository(session)

        # Create
        _, created = repo.upsert(
            name="workflow",
            asset_id=asset_id,
            steps=[{"name": "step1"}],
        )
        session.commit()
        assert created is True

        # Update
        workflow, created = repo.upsert(
            name="workflow",
            asset_id=asset_id,
            steps=[{"name": "step1"}, {"name": "step2"}],
        )
        session.commit()
        assert created is False
        assert len(workflow.steps) == 2


class TestSyncLogRepository:
    """Tests for SyncLogRepository."""

    def test_log_sync(self, session):
        """Test logging a sync operation."""
        repo = SyncLogRepository(session)

        log = repo.log_sync("agency", "test-agency", "success")
        session.commit()

        assert log.id is not None

    def test_get_recent(self, session):
        """Test getting recent sync logs."""
        repo = SyncLogRepository(session)

        repo.log_sync("agency", "agency-1", "success")
        repo.log_sync("asset", "asset-1", "success")
        repo.log_sync("workflow", "workflow-1", "error", "Some error")
        session.commit()

        logs = repo.get_recent(limit=10)
        assert len(logs) == 3

    def test_get_errors(self, session):
        """Test getting error logs."""
        repo = SyncLogRepository(session)

        repo.log_sync("agency", "agency-1", "success")
        repo.log_sync("asset", "asset-1", "error", "Error 1")
        repo.log_sync("workflow", "workflow-1", "error", "Error 2")
        session.commit()

        errors = repo.get_errors(limit=10)
        assert len(errors) == 2
