"""Registry loader and validator for manifest files."""

from pathlib import Path

import yaml
from pydantic import ValidationError

from control.models import Agency, Asset, Workflow


class RegistryError(Exception):
    """Error during registry operations."""

    pass


class Registry:
    """Loads and manages manifest registries."""

    def __init__(self, manifests_path: Path | str) -> None:
        """Initialize registry with path to manifests directory.

        Args:
            manifests_path: Path to the manifests directory containing
                           agencies/, assets/, and workflows/ subdirectories.
        """
        self.manifests_path = Path(manifests_path)
        self._agencies: dict[str, Agency] = {}
        self._assets: dict[str, Asset] = {}
        self._workflows: dict[str, Workflow] = {}
        self._loaded = False

    def load(self) -> None:
        """Load all manifests from disk."""
        self._load_agencies()
        self._load_assets()
        self._load_workflows()
        self._validate_references()
        self._loaded = True

    def _load_yaml_files(self, subdir: str) -> list[dict]:
        """Load all YAML files from a subdirectory.

        Supports two formats:
        1. Single file with list of items (e.g., agencies.yaml containing multiple agencies)
        2. Multiple files with one item each (e.g., census-bureau.yaml, bls.yaml)
        """
        path = self.manifests_path / subdir
        if not path.exists():
            return []

        results = []
        for yaml_file in list(path.glob("*.yaml")) + list(path.glob("*.yml")):
            with open(yaml_file) as f:
                data = yaml.safe_load(f)
                if data is None:
                    continue

                # Handle list format (multiple items in one file)
                if isinstance(data, list):
                    for item in data:
                        if item:
                            item["_source_file"] = str(yaml_file)
                            results.append(item)
                # Handle single item format
                else:
                    data["_source_file"] = str(yaml_file)
                    results.append(data)

        return results

    def _load_agencies(self) -> None:
        """Load all agency manifests."""
        for data in self._load_yaml_files("agencies"):
            source_file = data.pop("_source_file", "unknown")
            try:
                agency = Agency.model_validate(data)
                if agency.metadata.name in self._agencies:
                    raise RegistryError(
                        f"Duplicate agency name: {agency.metadata.name} "
                        f"(found in {source_file})"
                    )
                self._agencies[agency.metadata.name] = agency
            except ValidationError as e:
                raise RegistryError(f"Invalid agency manifest in {source_file}: {e}") from e

    def _load_assets(self) -> None:
        """Load all asset manifests."""
        for data in self._load_yaml_files("assets"):
            source_file = data.pop("_source_file", "unknown")
            try:
                asset = Asset.model_validate(data)
                if asset.metadata.name in self._assets:
                    raise RegistryError(
                        f"Duplicate asset name: {asset.metadata.name} " f"(found in {source_file})"
                    )
                self._assets[asset.metadata.name] = asset
            except ValidationError as e:
                raise RegistryError(f"Invalid asset manifest in {source_file}: {e}") from e

    def _load_workflows(self) -> None:
        """Load all workflow manifests."""
        for data in self._load_yaml_files("workflows"):
            source_file = data.pop("_source_file", "unknown")
            try:
                workflow = Workflow.model_validate(data)
                if workflow.metadata.name in self._workflows:
                    raise RegistryError(
                        f"Duplicate workflow name: {workflow.metadata.name} "
                        f"(found in {source_file})"
                    )
                self._workflows[workflow.metadata.name] = workflow
            except ValidationError as e:
                raise RegistryError(f"Invalid workflow manifest in {source_file}: {e}") from e

    def _validate_references(self) -> None:
        """Validate cross-references between registries."""
        # Validate asset -> agency references
        for asset_name, asset in self._assets.items():
            if asset.spec.agency_ref not in self._agencies:
                raise RegistryError(
                    f"Asset '{asset_name}' references unknown agency: "
                    f"'{asset.spec.agency_ref}'"
                )

        # Validate workflow -> asset references
        for workflow_name, workflow in self._workflows.items():
            if workflow.spec.asset_ref not in self._assets:
                raise RegistryError(
                    f"Workflow '{workflow_name}' references unknown asset: "
                    f"'{workflow.spec.asset_ref}'"
                )

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
