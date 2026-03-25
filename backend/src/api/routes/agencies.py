"""Agency API routes."""

from fastapi import APIRouter, HTTPException
from sqlalchemy import func, select

from src.api.deps import DBSession
from src.models.domain import AgencyModel, AssetModel
from src.models.schemas import AgencyDetailResponse, AgencyResponse

router = APIRouter()


@router.get("", response_model=list[AgencyResponse])
def list_agencies(db: DBSession) -> list[AgencyResponse]:
    """List all agencies with asset counts."""
    # Query agencies with asset count
    stmt = (
        select(
            AgencyModel,
            func.count(AssetModel.id).label("asset_count"),
        )
        .outerjoin(AssetModel)
        .group_by(AgencyModel.id)
        .order_by(AgencyModel.name)
    )

    results = db.execute(stmt).all()

    return [
        AgencyResponse(
            id=agency.id,
            name=agency.name,
            full_name=agency.full_name,
            base_url=agency.base_url,
            description=agency.description,
            labels=agency.labels,
            created_at=agency.created_at,
            updated_at=agency.updated_at,
            asset_count=asset_count,
        )
        for agency, asset_count in results
    ]


@router.get("/{name}", response_model=AgencyDetailResponse)
def get_agency(name: str, db: DBSession) -> AgencyDetailResponse:
    """Get agency details by name including its assets."""
    stmt = select(AgencyModel).where(AgencyModel.name == name)
    agency = db.execute(stmt).scalar_one_or_none()

    if not agency:
        raise HTTPException(status_code=404, detail=f"Agency '{name}' not found")

    return AgencyDetailResponse(
        id=agency.id,
        name=agency.name,
        full_name=agency.full_name,
        base_url=agency.base_url,
        description=agency.description,
        labels=agency.labels,
        created_at=agency.created_at,
        updated_at=agency.updated_at,
        asset_count=len(agency.assets),
        assets=[
            {"id": asset.id, "name": asset.name, "description": asset.description}
            for asset in agency.assets
        ],
    )
