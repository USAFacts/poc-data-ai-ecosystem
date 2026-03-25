"""Pydantic response models for the API."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict


class AgencyBase(BaseModel):
    """Base agency schema."""

    name: str
    full_name: str
    base_url: str | None = None
    description: str | None = None
    labels: dict[str, Any] | None = None


class AgencyResponse(AgencyBase):
    """Agency response with metadata."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    created_at: datetime
    updated_at: datetime
    asset_count: int = 0


class AgencyDetailResponse(AgencyResponse):
    """Agency response with assets."""

    assets: list["AssetSummary"] = []


class AssetBase(BaseModel):
    """Base asset schema."""

    name: str
    description: str | None = None
    acquisition_config: dict[str, Any]
    labels: dict[str, Any] | None = None


class AssetSummary(BaseModel):
    """Asset summary for listing."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    description: str | None = None


class AssetResponse(AssetBase):
    """Asset response with metadata."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    agency_id: int
    created_at: datetime
    updated_at: datetime
    workflow_count: int = 0


class AssetDetailResponse(AssetResponse):
    """Asset response with agency and workflows."""

    agency_name: str
    workflows: list["WorkflowSummary"] = []


class WorkflowBase(BaseModel):
    """Base workflow schema."""

    name: str
    steps: list[dict[str, Any]]


class WorkflowSummary(BaseModel):
    """Workflow summary for listing."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str


class WorkflowResponse(WorkflowBase):
    """Workflow response with metadata."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    asset_id: int
    created_at: datetime
    updated_at: datetime


class WorkflowDetailResponse(WorkflowResponse):
    """Workflow response with asset and agency info."""

    asset_name: str
    agency_name: str


class WorkflowRunRequest(BaseModel):
    """Request to trigger a workflow run."""

    dry_run: bool = False


class WorkflowRunResponse(BaseModel):
    """Response from workflow run trigger."""

    workflow_name: str
    status: str
    message: str


class DISScore(BaseModel):
    """Data Ingestion Score metrics."""

    dis_score: float
    quality_score: float
    efficiency_score: float
    execution_success_score: float


class WorkflowDISHistory(BaseModel):
    """DIS history for a specific workflow."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    workflow_name: str
    dis_score: float
    quality_score: float
    efficiency_score: float
    execution_success_score: float
    recorded_at: datetime


class OverallDISHistory(BaseModel):
    """Overall DIS history record."""

    model_config = ConfigDict(from_attributes=True)

    id: int
    overall_dis: float
    avg_quality: float
    avg_efficiency: float
    avg_execution_success: float
    workflow_count: int
    recorded_at: datetime


class MetricsSummary(BaseModel):
    """Summary of current metrics."""

    overall_dis: float
    avg_quality: float
    avg_efficiency: float
    avg_execution_success: float
    workflow_count: int
    latest_update: datetime | None = None


class MetricsHistoryResponse(BaseModel):
    """Response containing metrics history."""

    overall_history: list[OverallDISHistory]
    workflow_history: list[WorkflowDISHistory]


# Update forward references
AgencyDetailResponse.model_rebuild()
AssetDetailResponse.model_rebuild()
