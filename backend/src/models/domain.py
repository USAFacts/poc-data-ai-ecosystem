"""Domain models mirroring the ingestion-processing database models."""

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import DateTime, Float, JSON, ForeignKey, Integer, String, Text
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
        "AssetModel", back_populates="agency", lazy="selectin"
    )


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
        "WorkflowModel", back_populates="asset", lazy="selectin"
    )


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


class ExperimentModel(Base):
    """Experiment tracker model for RAG evaluation experiments."""

    __tablename__ = "experiments"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    status: Mapped[str] = mapped_column(String(50), default="pending")
    total_questions: Mapped[int] = mapped_column(Integer, default=0)
    completed_questions: Mapped[int] = mapped_column(Integer, default=0)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    aggregate_metrics: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    config: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)  # stores modes, sample_percent
    created_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    # Relationships
    results: Mapped[list["ExperimentResultModel"]] = relationship(
        "ExperimentResultModel", back_populates="experiment", lazy="selectin"
    )


class ExperimentResultModel(Base):
    """Individual question result within an experiment."""

    __tablename__ = "experiment_results"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    experiment_id: Mapped[int] = mapped_column(ForeignKey("experiments.id"), nullable=False)
    question_id: Mapped[str] = mapped_column(String(50), nullable=False)
    question_text: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[str] = mapped_column(String(100), nullable=False)
    mode: Mapped[str] = mapped_column(String(50), nullable=False)
    answer: Mapped[str] = mapped_column(Text, default="")
    confidence: Mapped[float] = mapped_column(Float, default=0.0)
    avg_relevance_score: Mapped[float] = mapped_column(Float, default=0.0)
    entity_coverage: Mapped[float] = mapped_column(Float, default=0.0)
    response_time_ms: Mapped[int] = mapped_column(Integer, default=0)
    input_tokens: Mapped[int] = mapped_column(Integer, default=0)
    output_tokens: Mapped[int] = mapped_column(Integer, default=0)
    total_tokens: Mapped[int] = mapped_column(Integer, default=0)
    documents_returned: Mapped[int] = mapped_column(Integer, default=0)
    sts: Mapped[float] = mapped_column(Float, default=0.0)
    nvs: Mapped[float] = mapped_column(Float, default=0.0)
    hds: Mapped[int] = mapped_column(Integer, default=0)
    cscs: Mapped[float] = mapped_column(Float, default=1.0)
    raw_result: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        default=lambda: datetime.now(timezone.utc)
    )

    # Relationships
    experiment: Mapped["ExperimentModel"] = relationship(
        "ExperimentModel", back_populates="results"
    )
