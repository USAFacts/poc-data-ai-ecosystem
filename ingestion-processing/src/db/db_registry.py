"""Database-backed registry implementing same interface as YAML Registry."""

from sqlalchemy.engine import Engine

from control.models import Agency, Asset, Workflow
from control.registry import RegistryError
from db.database import get_engine, get_session
from db.models import AgencyModel, AssetModel, WorkflowModel
from db.repository import AgencyRepository, AssetRepository, WorkflowRepository


class DbRegistry:
    """Database-backed registry implementing the same interface as Registry.

    Reads from the database instead of YAML files. Used by the Compiler
    for execution plan generation.
    """

    def __init__(self, engine: Engine | None = None) -> None:
        """Initialize DB registry.

        Args:
            engine: Optional SQLAlchemy engine. If None, uses default.
        """
        self.engine = engine or get_engine()
        self._agencies: dict[str, Agency] = {}
        self._assets: dict[str, Asset] = {}
        self._workflows: dict[str, Workflow] = {}
        self._loaded = False

    def load(self) -> None:
        """Load all entities from database."""
        with get_session(self.engine) as session:
            agency_repo = AgencyRepository(session)
            asset_repo = AssetRepository(session)
            workflow_repo = WorkflowRepository(session)

            # Build ID -> name maps for FK resolution
            agency_id_to_name: dict[int, str] = {}
            asset_id_to_name: dict[int, str] = {}

            # Load agencies
            for model in agency_repo.list_all():
                agency = self._model_to_agency(model)
                self._agencies[agency.metadata.name] = agency
                agency_id_to_name[model.id] = model.name

            # Load assets
            for model in asset_repo.list_all():
                agency_name = agency_id_to_name.get(model.agency_id)
                if agency_name is None:
                    raise RegistryError(
                        f"Asset '{model.name}' references unknown agency_id: {model.agency_id}"
                    )
                asset = self._model_to_asset(model, agency_name)
                self._assets[asset.metadata.name] = asset
                asset_id_to_name[model.id] = model.name

            # Load workflows
            for model in workflow_repo.list_all():
                asset_name = asset_id_to_name.get(model.asset_id)
                if asset_name is None:
                    raise RegistryError(
                        f"Workflow '{model.name}' references unknown asset_id: {model.asset_id}"
                    )
                workflow = self._model_to_workflow(model, asset_name)
                self._workflows[workflow.metadata.name] = workflow

        self._loaded = True

    @property
    def agencies(self) -> dict[str, Agency]:
        """Get all loaded agencies."""
        if not self._loaded:
            raise RegistryError("Registry not loaded. Call load() first.")
        return self._agencies

    @property
    def assets(self) -> dict[str, Asset]:
        """Get all loaded assets."""
        if not self._loaded:
            raise RegistryError("Registry not loaded. Call load() first.")
        return self._assets

    @property
    def workflows(self) -> dict[str, Workflow]:
        """Get all loaded workflows."""
        if not self._loaded:
            raise RegistryError("Registry not loaded. Call load() first.")
        return self._workflows

    def get_agency(self, name: str) -> Agency:
        """Get an agency by name."""
        if name not in self.agencies:
            raise RegistryError(f"Agency not found: {name}")
        return self.agencies[name]

    def get_asset(self, name: str) -> Asset:
        """Get an asset by name."""
        if name not in self.assets:
            raise RegistryError(f"Asset not found: {name}")
        return self.assets[name]

    def get_workflow(self, name: str) -> Workflow:
        """Get a workflow by name."""
        if name not in self.workflows:
            raise RegistryError(f"Workflow not found: {name}")
        return self.workflows[name]

    def get_asset_agency(self, asset: Asset) -> Agency:
        """Get the agency for an asset."""
        return self.get_agency(asset.spec.agency_ref)

    def get_workflow_asset(self, workflow: Workflow) -> Asset:
        """Get the asset for a workflow."""
        return self.get_asset(workflow.spec.asset_ref)

    def get_workflow_agency(self, workflow: Workflow) -> Agency:
        """Get the agency for a workflow (via its asset)."""
        asset = self.get_workflow_asset(workflow)
        return self.get_asset_agency(asset)

    def _model_to_agency(self, model: AgencyModel) -> Agency:
        """Convert AgencyModel to Agency Pydantic model."""
        return Agency.model_validate({
            "apiVersion": "pipeline/v1",
            "kind": "Agency",
            "metadata": {
                "name": model.name,
                "labels": model.labels or {},
            },
            "spec": {
                "fullName": model.full_name,
                "baseUrl": model.base_url or "",
                "description": model.description or "",
            },
        })

    def _model_to_asset(self, model: AssetModel, agency_name: str) -> Asset:
        """Convert AssetModel to Asset Pydantic model."""
        # Build the acquisition config dict for validation
        acq_dict = self._build_acquisition_dict(model.acquisition_config)

        return Asset.model_validate({
            "apiVersion": "pipeline/v1",
            "kind": "Asset",
            "metadata": {
                "name": model.name,
                "labels": model.labels or {},
            },
            "spec": {
                "agencyRef": agency_name,
                "description": model.description or "",
                "acquisition": acq_dict,
            },
        })

    def _model_to_workflow(self, model: WorkflowModel, asset_name: str) -> Workflow:
        """Convert WorkflowModel to Workflow Pydantic model."""
        return Workflow.model_validate({
            "apiVersion": "pipeline/v1",
            "kind": "Workflow",
            "metadata": {
                "name": model.name,
                "labels": {},
            },
            "spec": {
                "assetRef": asset_name,
                "steps": model.steps,  # Already in correct format with dependsOn
            },
        })

    def _build_acquisition_dict(self, config: dict) -> dict:
        """Build acquisition config dict for Pydantic validation.

        The config is already stored in the correct format, so we just
        pass it through. The storage format matches the YAML format.
        """
        return config
