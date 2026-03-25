"""Asset API routes."""

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import func, select

from src.api.deps import DBSession
from src.models.domain import AgencyModel, AssetModel, WorkflowModel
from src.models.schemas import AssetDetailResponse, AssetResponse

router = APIRouter()


@router.get("", response_model=list[AssetResponse])
def list_assets(
    db: DBSession,
    agency: str | None = Query(None, description="Filter by agency name"),
) -> list[AssetResponse]:
    """List all assets with workflow counts, optionally filtered by agency."""
    stmt = (
        select(
            AssetModel,
            func.count(WorkflowModel.id).label("workflow_count"),
        )
        .outerjoin(WorkflowModel)
        .group_by(AssetModel.id)
        .order_by(AssetModel.name)
    )

    if agency:
        stmt = stmt.join(AgencyModel).where(AgencyModel.name == agency)

    results = db.execute(stmt).all()

    return [
        AssetResponse(
            id=asset.id,
            name=asset.name,
            description=asset.description,
            acquisition_config=asset.acquisition_config,
            labels=asset.labels,
            agency_id=asset.agency_id,
            created_at=asset.created_at,
            updated_at=asset.updated_at,
            workflow_count=workflow_count,
        )
        for asset, workflow_count in results
    ]


@router.get("/{name}", response_model=AssetDetailResponse)
def get_asset(name: str, db: DBSession) -> AssetDetailResponse:
    """Get asset details by name including its workflows."""
    stmt = select(AssetModel).where(AssetModel.name == name)
    asset = db.execute(stmt).scalar_one_or_none()

    if not asset:
        raise HTTPException(status_code=404, detail=f"Asset '{name}' not found")

    return AssetDetailResponse(
        id=asset.id,
        name=asset.name,
        description=asset.description,
        acquisition_config=asset.acquisition_config,
        labels=asset.labels,
        agency_id=asset.agency_id,
        created_at=asset.created_at,
        updated_at=asset.updated_at,
        workflow_count=len(asset.workflows),
        agency_name=asset.agency.name,
        workflows=[
            {"id": workflow.id, "name": workflow.name}
            for workflow in asset.workflows
        ],
    )
