"""Pydantic models for control plane registries."""

from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field


class AcquisitionType(str, Enum):
    """Type of data acquisition."""

    HTTP = "http"
    API = "api"


class TemporalPattern(str, Enum):
    """Temporal enumeration patterns for URL templates."""

    FISCAL_YEAR_QUARTER = "fiscal_year_quarter"  # fy2025_q3
    CALENDAR_YEAR_QUARTER = "calendar_year_quarter"  # 2025_q3
    CALENDAR_YEAR_MONTH = "calendar_year_month"  # 2025_01
    CALENDAR_YEAR = "calendar_year"  # 2025


class TemporalConfig(BaseModel):
    """Configuration for temporal URL enumeration.

    Supports automatic resolution of URL templates based on current date.
    Example: quarterly_all_forms_fy{fiscal_year}_q{quarter}.xlsx
    """

    pattern: TemporalPattern = Field(
        ..., description="The temporal pattern to use for URL resolution"
    )
    fiscal_year_start_month: int = Field(
        10,
        alias="fiscalYearStartMonth",
        ge=1,
        le=12,
        description="Month when fiscal year starts (default: 10 for US federal)",
    )
    url_template: str = Field(
        ...,
        alias="urlTemplate",
        description="URL template with placeholders: {fiscal_year}, {quarter}, {year}, {month}",
    )


class AuthType(str, Enum):
    """Type of authentication for API sources."""

    API_KEY = "api_key"
    BEARER = "bearer"
    BASIC = "basic"


class AuthConfig(BaseModel):
    """Authentication configuration for API sources."""

    type: AuthType
    key_env_var: str | None = Field(None, description="Environment variable containing API key")
    header_name: str = Field("X-API-Key", description="Header name for API key auth")


class HttpSource(BaseModel):
    """HTTP source configuration.

    Supports either a static URL or a temporal URL template for predictable
    temporal enumeration patterns (e.g., quarterly government data releases).
    """

    url: str | None = Field(None, description="Static URL to download from")
    temporal: TemporalConfig | None = Field(
        None, description="Temporal URL template configuration for predictable patterns"
    )
    headers: dict[str, str] = Field(default_factory=dict, description="Additional HTTP headers")

    def model_post_init(self, __context: Any) -> None:
        """Validate that either url or temporal is provided."""
        if self.url is None and self.temporal is None:
            raise ValueError("Either 'url' or 'temporal' must be provided")
        if self.url is not None and self.temporal is not None:
            raise ValueError("Cannot specify both 'url' and 'temporal'")


class ApiSource(BaseModel):
    """API source configuration."""

    base_url: str = Field(..., description="Base URL of the API")
    endpoint: str = Field(..., description="API endpoint path")
    method: str = Field("GET", description="HTTP method")
    params: dict[str, str] = Field(default_factory=dict, description="Query parameters")
    headers: dict[str, str] = Field(default_factory=dict, description="Additional HTTP headers")
    auth: AuthConfig | None = Field(None, description="Authentication configuration")


class AcquisitionConfig(BaseModel):
    """Configuration for data acquisition."""

    type: AcquisitionType
    source: HttpSource | ApiSource
    format: str = Field("csv", description="Expected data format")
    schedule: str | None = Field(None, description="Cron-like schedule expression")


class Metadata(BaseModel):
    """Common metadata for all resources."""

    name: str = Field(..., description="Unique identifier for this resource")
    labels: dict[str, str] = Field(default_factory=dict, description="Labels for categorization")


class AgencySpec(BaseModel):
    """Specification for a government agency."""

    full_name: str = Field(..., alias="fullName", description="Full name of the agency")
    base_url: str = Field(..., alias="baseUrl", description="Base URL of the agency")
    description: str = Field("", description="Description of the agency")


class Agency(BaseModel):
    """Government agency resource."""

    api_version: str = Field("pipeline/v1", alias="apiVersion")
    kind: Literal["Agency"] = "Agency"
    metadata: Metadata
    spec: AgencySpec


class AssetSpec(BaseModel):
    """Specification for a data asset."""

    agency_ref: str = Field(..., alias="agencyRef", description="Reference to agency name")
    description: str = Field("", description="Description of the asset")
    acquisition: AcquisitionConfig


class Asset(BaseModel):
    """Data asset resource."""

    api_version: str = Field("pipeline/v1", alias="apiVersion")
    kind: Literal["Asset"] = "Asset"
    metadata: Metadata
    spec: AssetSpec


class StepConfig(BaseModel):
    """Configuration for a workflow step."""

    name: str = Field(..., description="Step name within the workflow")
    type: str = Field(..., description="Step type (e.g., 'acquisition', 'validation')")
    config: dict[str, Any] = Field(default_factory=dict, description="Step-specific configuration")
    depends_on: list[str] = Field(
        default_factory=list, alias="dependsOn", description="Steps this step depends on"
    )


class WorkflowSpec(BaseModel):
    """Specification for a workflow."""

    asset_ref: str = Field(..., alias="assetRef", description="Reference to asset name")
    steps: list[StepConfig] = Field(..., description="Ordered list of steps to execute")


class Workflow(BaseModel):
    """Workflow resource defining processing pipeline."""

    api_version: str = Field("pipeline/v1", alias="apiVersion")
    kind: Literal["Workflow"] = "Workflow"
    metadata: Metadata
    spec: WorkflowSpec

    def get_step(self, name: str) -> StepConfig | None:
        """Get a step by name."""
        for step in self.spec.steps:
            if step.name == name:
                return step
        return None
