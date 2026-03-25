"""Workflow API routes."""

from fastapi import APIRouter, HTTPException
from sqlalchemy import select

from src.api.deps import DBSession
from src.models.domain import AssetModel, WorkflowModel
from src.models.schemas import (
    WorkflowDetailResponse,
    WorkflowResponse,
    WorkflowRunRequest,
    WorkflowRunResponse,
)

router = APIRouter()


@router.get("", response_model=list[WorkflowResponse])
def list_workflows(db: DBSession) -> list[WorkflowResponse]:
    """List all workflows."""
    stmt = select(WorkflowModel).order_by(WorkflowModel.name)
    workflows = db.execute(stmt).scalars().all()

    return [
        WorkflowResponse(
            id=workflow.id,
            name=workflow.name,
            steps=workflow.steps,
            asset_id=workflow.asset_id,
            created_at=workflow.created_at,
            updated_at=workflow.updated_at,
        )
        for workflow in workflows
    ]


@router.get("/{name}", response_model=WorkflowDetailResponse)
def get_workflow(name: str, db: DBSession) -> WorkflowDetailResponse:
    """Get workflow details by name."""
    stmt = select(WorkflowModel).where(WorkflowModel.name == name)
    workflow = db.execute(stmt).scalar_one_or_none()

    if not workflow:
        raise HTTPException(status_code=404, detail=f"Workflow '{name}' not found")

    # Get asset and agency info
    asset_stmt = select(AssetModel).where(AssetModel.id == workflow.asset_id)
    asset = db.execute(asset_stmt).scalar_one()

    return WorkflowDetailResponse(
        id=workflow.id,
        name=workflow.name,
        steps=workflow.steps,
        asset_id=workflow.asset_id,
        created_at=workflow.created_at,
        updated_at=workflow.updated_at,
        asset_name=asset.name,
        agency_name=asset.agency.name,
    )


@router.post("/{name}/run", response_model=WorkflowRunResponse)
def run_workflow(name: str, request: WorkflowRunRequest, db: DBSession) -> WorkflowRunResponse:
    """Trigger a workflow execution.

    Note: This is a placeholder. Actual workflow execution would need
    to be implemented by calling the ingestion-processing CLI or
    implementing a proper task queue.
    """
    # Verify workflow exists
    stmt = select(WorkflowModel).where(WorkflowModel.name == name)
    workflow = db.execute(stmt).scalar_one_or_none()

    if not workflow:
        raise HTTPException(status_code=404, detail=f"Workflow '{name}' not found")

    if request.dry_run:
        return WorkflowRunResponse(
            workflow_name=name,
            status="dry_run",
            message=f"Dry run: Workflow '{name}' would be executed with {len(workflow.steps)} steps",
        )

    # TODO: Implement actual workflow execution
    # This would typically involve:
    # 1. Adding a task to a job queue (Celery, RQ, etc.)
    # 2. Or calling the ingestion-processing CLI directly
    # 3. Or triggering a subprocess

    return WorkflowRunResponse(
        workflow_name=name,
        status="queued",
        message=f"Workflow '{name}' has been queued for execution. "
        "Note: Full execution support requires additional infrastructure.",
    )
