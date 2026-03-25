"""SQLAlchemy ORM models for pipeline manifests."""

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import JSON, ForeignKey, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base class for all ORM models."""

    pass


class AgencyModel(Base):
    """Agency database model."""

    __tablename__ = "agencies"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    full_name: Mapped[str] = mapped_column(String(500), nullable=False)
    base_url: Mapped[str | None] = mapped_column(String(1000))
    description: Mapped[str | None] = mapped_column(Text)
    labels: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    # Relationships
    assets: Mapped[list["AssetModel"]] = relationship(
        "AssetModel", back_populates="agency", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Agency(id={self.id}, name='{self.name}')>"


class AssetModel(Base):
    """Asset database model."""

    __tablename__ = "assets"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    agency_id: Mapped[int] = mapped_column(ForeignKey("agencies.id"), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    acquisition_config: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    labels: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    # Relationships
    agency: Mapped["AgencyModel"] = relationship("AgencyModel", back_populates="assets")
    workflows: Mapped[list["WorkflowModel"]] = relationship(
        "WorkflowModel", back_populates="asset", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Asset(id={self.id}, name='{self.name}')>"


class WorkflowModel(Base):
    """Workflow database model."""

    __tablename__ = "workflows"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    asset_id: Mapped[int] = mapped_column(ForeignKey("assets.id"), nullable=False)
    steps: Mapped[list[dict[str, Any]]] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    # Relationships
    asset: Mapped["AssetModel"] = relationship("AssetModel", back_populates="workflows")

    def __repr__(self) -> str:
        return f"<Workflow(id={self.id}, name='{self.name}')>"


class SyncLogModel(Base):
    """Sync log database model for tracking sync operations."""

    __tablename__ = "sync_log"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    entity_type: Mapped[str] = mapped_column(String(50), nullable=False)
    entity_name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(50), nullable=False)
    error_message: Mapped[str | None] = mapped_column(Text)
    synced_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc)
    )

    def __repr__(self) -> str:
        return (
            f"<SyncLog(id={self.id}, entity_type='{self.entity_type}', "
            f"entity_name='{self.entity_name}', status='{self.status}')>"
        )


class DISHistoryModel(Base):
    """Data Ingestion Score history for tracking trends over time."""

    __tablename__ = "dis_history"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    workflow_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    dis_score: Mapped[float] = mapped_column(nullable=False)
    quality_score: Mapped[float] = mapped_column(nullable=False)
    efficiency_score: Mapped[float] = mapped_column(nullable=False)
    execution_success_score: Mapped[float] = mapped_column(nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc),
        index=True,
    )

    def __repr__(self) -> str:
        return (
            f"<DISHistory(id={self.id}, workflow='{self.workflow_name}', "
            f"dis={self.dis_score:.1f}, recorded_at='{self.recorded_at}')>"
        )


class PipelineLogModel(Base):
    """Structured pipeline log entries stored in the database."""

    __tablename__ = "pipeline_logs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc), index=True
    )
    level: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    logger_name: Mapped[str] = mapped_column(String(255), nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    run_id: Mapped[str | None] = mapped_column(String(255), index=True)
    workflow: Mapped[str | None] = mapped_column(String(255), index=True)
    step: Mapped[str | None] = mapped_column(String(255))
    asset: Mapped[str | None] = mapped_column(String(255))
    extra: Mapped[dict[str, Any] | None] = mapped_column(JSON)

    def __repr__(self) -> str:
        return (
            f"<PipelineLog(id={self.id}, level='{self.level}', "
            f"run_id='{self.run_id}', logger='{self.logger_name}', ts='{self.timestamp}')>"
        )


class DISOverallHistoryModel(Base):
    """Overall Data Ingestion Score history for tracking aggregate trends."""

    __tablename__ = "dis_overall_history"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    overall_dis: Mapped[float] = mapped_column(nullable=False)
    avg_quality: Mapped[float] = mapped_column(nullable=False)
    avg_efficiency: Mapped[float] = mapped_column(nullable=False)
    avg_execution_success: Mapped[float] = mapped_column(nullable=False)
    workflow_count: Mapped[int] = mapped_column(nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc),
        index=True,
    )

    def __repr__(self) -> str:
        return (
            f"<DISOverallHistory(id={self.id}, overall_dis={self.overall_dis:.1f}, "
            f"recorded_at='{self.recorded_at}')>"
        )
