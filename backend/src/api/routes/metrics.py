"""Metrics API routes for DIS (Data Ingestion Score) data."""

from fastapi import APIRouter, Query
from sqlalchemy import desc, select

from src.api.deps import DBSession
from src.models.domain import DISHistoryModel, DISOverallHistoryModel
from src.models.schemas import (
    MetricsHistoryResponse,
    MetricsSummary,
    OverallDISHistory,
    WorkflowDISHistory,
)

router = APIRouter()


@router.get("", response_model=MetricsSummary)
def get_metrics_summary(db: DBSession) -> MetricsSummary:
    """Get current overall metrics summary."""
    # Get the latest overall DIS record
    stmt = select(DISOverallHistoryModel).order_by(desc(DISOverallHistoryModel.recorded_at)).limit(1)
    latest = db.execute(stmt).scalar_one_or_none()

    if not latest:
        return MetricsSummary(
            overall_dis=0.0,
            avg_quality=0.0,
            avg_efficiency=0.0,
            avg_execution_success=0.0,
            workflow_count=0,
            latest_update=None,
        )

    return MetricsSummary(
        overall_dis=latest.overall_dis,
        avg_quality=latest.avg_quality,
        avg_efficiency=latest.avg_efficiency,
        avg_execution_success=latest.avg_execution_success,
        workflow_count=latest.workflow_count,
        latest_update=latest.recorded_at,
    )


@router.get("/history", response_model=MetricsHistoryResponse)
def get_metrics_history(
    db: DBSession,
    workflow: str | None = Query(None, description="Filter by workflow name"),
    limit: int = Query(100, description="Maximum number of records to return"),
) -> MetricsHistoryResponse:
    """Get metrics history over time."""
    # Get overall history
    overall_stmt = (
        select(DISOverallHistoryModel)
        .order_by(desc(DISOverallHistoryModel.recorded_at))
        .limit(limit)
    )
    overall_records = db.execute(overall_stmt).scalars().all()

    # Get workflow history
    workflow_stmt = select(DISHistoryModel).order_by(desc(DISHistoryModel.recorded_at))

    if workflow:
        workflow_stmt = workflow_stmt.where(DISHistoryModel.workflow_name == workflow)

    workflow_stmt = workflow_stmt.limit(limit)
    workflow_records = db.execute(workflow_stmt).scalars().all()

    return MetricsHistoryResponse(
        overall_history=[
            OverallDISHistory(
                id=record.id,
                overall_dis=record.overall_dis,
                avg_quality=record.avg_quality,
                avg_efficiency=record.avg_efficiency,
                avg_execution_success=record.avg_execution_success,
                workflow_count=record.workflow_count,
                recorded_at=record.recorded_at,
            )
            for record in overall_records
        ],
        workflow_history=[
            WorkflowDISHistory(
                id=record.id,
                workflow_name=record.workflow_name,
                dis_score=record.dis_score,
                quality_score=record.quality_score,
                efficiency_score=record.efficiency_score,
                execution_success_score=record.execution_success_score,
                recorded_at=record.recorded_at,
            )
            for record in workflow_records
        ],
    )
