"""Repository classes for database CRUD operations."""

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from db.models import (
    AgencyModel,
    AssetModel,
    DISHistoryModel,
    DISOverallHistoryModel,
    SyncLogModel,
    WorkflowModel,
)


class AgencyRepository:
    """Repository for Agency CRUD operations."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def get_by_id(self, agency_id: int) -> AgencyModel | None:
        """Get agency by ID."""
        return self.session.get(AgencyModel, agency_id)

    def get_by_name(self, name: str) -> AgencyModel | None:
        """Get agency by name."""
        stmt = select(AgencyModel).where(AgencyModel.name == name)
        return self.session.execute(stmt).scalar_one_or_none()

    def list_all(self) -> list[AgencyModel]:
        """List all agencies."""
        stmt = select(AgencyModel).order_by(AgencyModel.name)
        return list(self.session.execute(stmt).scalars().all())

    def create(
        self,
        name: str,
        full_name: str,
        base_url: str | None = None,
        description: str | None = None,
        labels: dict[str, Any] | None = None,
    ) -> AgencyModel:
        """Create a new agency."""
        agency = AgencyModel(
            name=name,
            full_name=full_name,
            base_url=base_url,
            description=description,
            labels=labels,
        )
        self.session.add(agency)
        self.session.flush()  # Get the ID
        return agency

    def update(
        self,
        agency: AgencyModel,
        full_name: str | None = None,
        base_url: str | None = None,
        description: str | None = None,
        labels: dict[str, Any] | None = None,
    ) -> AgencyModel:
        """Update an existing agency."""
        if full_name is not None:
            agency.full_name = full_name
        if base_url is not None:
            agency.base_url = base_url
        if description is not None:
            agency.description = description
        if labels is not None:
            agency.labels = labels
        agency.updated_at = datetime.now(timezone.utc)
        self.session.flush()
        return agency

    def delete(self, agency: AgencyModel) -> None:
        """Delete an agency."""
        self.session.delete(agency)
        self.session.flush()

    def upsert(
        self,
        name: str,
        full_name: str,
        base_url: str | None = None,
        description: str | None = None,
        labels: dict[str, Any] | None = None,
    ) -> tuple[AgencyModel, bool]:
        """Insert or update an agency.

        Returns:
            Tuple of (agency, created) where created is True if a new
            agency was created, False if an existing one was updated.
        """
        existing = self.get_by_name(name)
        if existing:
            self.update(existing, full_name, base_url, description, labels)
            return existing, False
        else:
            agency = self.create(name, full_name, base_url, description, labels)
            return agency, True


class AssetRepository:
    """Repository for Asset CRUD operations."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def get_by_id(self, asset_id: int) -> AssetModel | None:
        """Get asset by ID."""
        return self.session.get(AssetModel, asset_id)

    def get_by_name(self, name: str) -> AssetModel | None:
        """Get asset by name."""
        stmt = select(AssetModel).where(AssetModel.name == name)
        return self.session.execute(stmt).scalar_one_or_none()

    def list_all(self) -> list[AssetModel]:
        """List all assets."""
        stmt = select(AssetModel).order_by(AssetModel.name)
        return list(self.session.execute(stmt).scalars().all())

    def list_by_agency(self, agency_id: int) -> list[AssetModel]:
        """List all assets for an agency."""
        stmt = (
            select(AssetModel)
            .where(AssetModel.agency_id == agency_id)
            .order_by(AssetModel.name)
        )
        return list(self.session.execute(stmt).scalars().all())

    def create(
        self,
        name: str,
        agency_id: int,
        acquisition_config: dict[str, Any],
        description: str | None = None,
        labels: dict[str, Any] | None = None,
    ) -> AssetModel:
        """Create a new asset."""
        asset = AssetModel(
            name=name,
            agency_id=agency_id,
            acquisition_config=acquisition_config,
            description=description,
            labels=labels,
        )
        self.session.add(asset)
        self.session.flush()
        return asset

    def update(
        self,
        asset: AssetModel,
        agency_id: int | None = None,
        acquisition_config: dict[str, Any] | None = None,
        description: str | None = None,
        labels: dict[str, Any] | None = None,
    ) -> AssetModel:
        """Update an existing asset."""
        if agency_id is not None:
            asset.agency_id = agency_id
        if acquisition_config is not None:
            asset.acquisition_config = acquisition_config
        if description is not None:
            asset.description = description
        if labels is not None:
            asset.labels = labels
        asset.updated_at = datetime.now(timezone.utc)
        self.session.flush()
        return asset

    def delete(self, asset: AssetModel) -> None:
        """Delete an asset."""
        self.session.delete(asset)
        self.session.flush()

    def upsert(
        self,
        name: str,
        agency_id: int,
        acquisition_config: dict[str, Any],
        description: str | None = None,
        labels: dict[str, Any] | None = None,
    ) -> tuple[AssetModel, bool]:
        """Insert or update an asset.

        Returns:
            Tuple of (asset, created) where created is True if a new
            asset was created, False if an existing one was updated.
        """
        existing = self.get_by_name(name)
        if existing:
            self.update(existing, agency_id, acquisition_config, description, labels)
            return existing, False
        else:
            asset = self.create(name, agency_id, acquisition_config, description, labels)
            return asset, True


class WorkflowRepository:
    """Repository for Workflow CRUD operations."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def get_by_id(self, workflow_id: int) -> WorkflowModel | None:
        """Get workflow by ID."""
        return self.session.get(WorkflowModel, workflow_id)

    def get_by_name(self, name: str) -> WorkflowModel | None:
        """Get workflow by name."""
        stmt = select(WorkflowModel).where(WorkflowModel.name == name)
        return self.session.execute(stmt).scalar_one_or_none()

    def list_all(self) -> list[WorkflowModel]:
        """List all workflows."""
        stmt = select(WorkflowModel).order_by(WorkflowModel.name)
        return list(self.session.execute(stmt).scalars().all())

    def list_by_asset(self, asset_id: int) -> list[WorkflowModel]:
        """List all workflows for an asset."""
        stmt = (
            select(WorkflowModel)
            .where(WorkflowModel.asset_id == asset_id)
            .order_by(WorkflowModel.name)
        )
        return list(self.session.execute(stmt).scalars().all())

    def create(
        self,
        name: str,
        asset_id: int,
        steps: list[dict[str, Any]],
    ) -> WorkflowModel:
        """Create a new workflow."""
        workflow = WorkflowModel(
            name=name,
            asset_id=asset_id,
            steps=steps,
        )
        self.session.add(workflow)
        self.session.flush()
        return workflow

    def update(
        self,
        workflow: WorkflowModel,
        asset_id: int | None = None,
        steps: list[dict[str, Any]] | None = None,
    ) -> WorkflowModel:
        """Update an existing workflow."""
        if asset_id is not None:
            workflow.asset_id = asset_id
        if steps is not None:
            workflow.steps = steps
        workflow.updated_at = datetime.now(timezone.utc)
        self.session.flush()
        return workflow

    def delete(self, workflow: WorkflowModel) -> None:
        """Delete a workflow."""
        self.session.delete(workflow)
        self.session.flush()

    def upsert(
        self,
        name: str,
        asset_id: int,
        steps: list[dict[str, Any]],
    ) -> tuple[WorkflowModel, bool]:
        """Insert or update a workflow.

        Returns:
            Tuple of (workflow, created) where created is True if a new
            workflow was created, False if an existing one was updated.
        """
        existing = self.get_by_name(name)
        if existing:
            self.update(existing, asset_id, steps)
            return existing, False
        else:
            workflow = self.create(name, asset_id, steps)
            return workflow, True


class SyncLogRepository:
    """Repository for SyncLog operations."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def log_sync(
        self,
        entity_type: str,
        entity_name: str,
        status: str,
        error_message: str | None = None,
    ) -> SyncLogModel:
        """Log a sync operation."""
        log = SyncLogModel(
            entity_type=entity_type,
            entity_name=entity_name,
            status=status,
            error_message=error_message,
        )
        self.session.add(log)
        self.session.flush()
        return log

    def get_recent(self, limit: int = 50) -> list[SyncLogModel]:
        """Get recent sync logs."""
        stmt = select(SyncLogModel).order_by(SyncLogModel.synced_at.desc()).limit(limit)
        return list(self.session.execute(stmt).scalars().all())

    def get_by_entity(
        self, entity_type: str, entity_name: str, limit: int = 10
    ) -> list[SyncLogModel]:
        """Get sync logs for a specific entity."""
        stmt = (
            select(SyncLogModel)
            .where(
                SyncLogModel.entity_type == entity_type,
                SyncLogModel.entity_name == entity_name,
            )
            .order_by(SyncLogModel.synced_at.desc())
            .limit(limit)
        )
        return list(self.session.execute(stmt).scalars().all())

    def get_errors(self, limit: int = 50) -> list[SyncLogModel]:
        """Get recent error logs."""
        stmt = (
            select(SyncLogModel)
            .where(SyncLogModel.status == "error")
            .order_by(SyncLogModel.synced_at.desc())
            .limit(limit)
        )
        return list(self.session.execute(stmt).scalars().all())


class DISHistoryRepository:
    """Repository for Data Ingestion Score history operations."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def record_workflow_dis(
        self,
        workflow_name: str,
        dis_score: float,
        quality_score: float,
        efficiency_score: float,
        execution_success_score: float,
    ) -> DISHistoryModel:
        """Record a DIS score for a workflow."""
        record = DISHistoryModel(
            workflow_name=workflow_name,
            dis_score=dis_score,
            quality_score=quality_score,
            efficiency_score=efficiency_score,
            execution_success_score=execution_success_score,
        )
        self.session.add(record)
        self.session.flush()
        return record

    def record_overall_dis(
        self,
        overall_dis: float,
        avg_quality: float,
        avg_efficiency: float,
        avg_execution_success: float,
        workflow_count: int,
    ) -> DISOverallHistoryModel:
        """Record an overall DIS score."""
        record = DISOverallHistoryModel(
            overall_dis=overall_dis,
            avg_quality=avg_quality,
            avg_efficiency=avg_efficiency,
            avg_execution_success=avg_execution_success,
            workflow_count=workflow_count,
        )
        self.session.add(record)
        self.session.flush()
        return record

    def get_previous_workflow_dis(self, workflow_name: str) -> DISHistoryModel | None:
        """Get the most recent previous DIS record for a workflow.

        Skips the very latest record to get the previous one for comparison.
        """
        stmt = (
            select(DISHistoryModel)
            .where(DISHistoryModel.workflow_name == workflow_name)
            .order_by(DISHistoryModel.recorded_at.desc())
            .offset(1)  # Skip the current record
            .limit(1)
        )
        return self.session.execute(stmt).scalar_one_or_none()

    def get_latest_workflow_dis(self, workflow_name: str) -> DISHistoryModel | None:
        """Get the latest DIS record for a workflow."""
        stmt = (
            select(DISHistoryModel)
            .where(DISHistoryModel.workflow_name == workflow_name)
            .order_by(DISHistoryModel.recorded_at.desc())
            .limit(1)
        )
        return self.session.execute(stmt).scalar_one_or_none()

    def get_previous_overall_dis(self) -> DISOverallHistoryModel | None:
        """Get the most recent previous overall DIS record.

        Skips the very latest record to get the previous one for comparison.
        """
        stmt = (
            select(DISOverallHistoryModel)
            .order_by(DISOverallHistoryModel.recorded_at.desc())
            .offset(1)  # Skip the current record
            .limit(1)
        )
        return self.session.execute(stmt).scalar_one_or_none()

    def get_latest_overall_dis(self) -> DISOverallHistoryModel | None:
        """Get the latest overall DIS record."""
        stmt = (
            select(DISOverallHistoryModel)
            .order_by(DISOverallHistoryModel.recorded_at.desc())
            .limit(1)
        )
        return self.session.execute(stmt).scalar_one_or_none()

    def get_workflow_history(
        self, workflow_name: str, limit: int = 30
    ) -> list[DISHistoryModel]:
        """Get DIS history for a workflow."""
        stmt = (
            select(DISHistoryModel)
            .where(DISHistoryModel.workflow_name == workflow_name)
            .order_by(DISHistoryModel.recorded_at.desc())
            .limit(limit)
        )
        return list(self.session.execute(stmt).scalars().all())

    def get_overall_history(self, limit: int = 30) -> list[DISOverallHistoryModel]:
        """Get overall DIS history."""
        stmt = (
            select(DISOverallHistoryModel)
            .order_by(DISOverallHistoryModel.recorded_at.desc())
            .limit(limit)
        )
        return list(self.session.execute(stmt).scalars().all())
