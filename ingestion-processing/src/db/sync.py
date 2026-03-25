"""Manifest to database sync orchestrator."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pydantic import ValidationError
from sqlalchemy.engine import Engine

from control.models import Agency, Asset, Workflow
from db.database import get_engine, get_session, init_db
from db.repository import (
    AgencyRepository,
    AssetRepository,
    SyncLogRepository,
    WorkflowRepository,
)
from logging_manager import get_logger

logger = get_logger(__name__)


@dataclass
class SyncError:
    """Details about a sync error."""

    entity_type: str
    entity_name: str
    error: str


@dataclass
class SyncReport:
    """Report of sync operation results."""

    agencies_synced: int = 0
    agencies_errors: int = 0
    assets_synced: int = 0
    assets_errors: int = 0
    workflows_synced: int = 0
    workflows_errors: int = 0
    errors: list[SyncError] = field(default_factory=list)

    @property
    def total_synced(self) -> int:
        """Total entities synced."""
        return self.agencies_synced + self.assets_synced + self.workflows_synced

    @property
    def total_errors(self) -> int:
        """Total errors encountered."""
        return self.agencies_errors + self.assets_errors + self.workflows_errors

    @property
    def success(self) -> bool:
        """Whether the sync had any errors."""
        return self.total_errors == 0


class ManifestSync:
    """Synchronizes YAML manifests to database.

    Handles sync in dependency order:
    1. Agencies (no FK dependencies)
    2. Assets (depends on agencies)
    3. Workflows (depends on assets)
    """

    def __init__(
        self,
        manifests_path: Path | str,
        engine: Engine | None = None,
    ) -> None:
        """Initialize sync orchestrator.

        Args:
            manifests_path: Path to manifests directory.
            engine: Optional SQLAlchemy engine. If None, uses default.
        """
        self.manifests_path = Path(manifests_path)
        self.engine = engine or get_engine()

    def sync(self) -> SyncReport:
        """Sync all manifests to database.

        Returns:
            SyncReport with counts and any errors.
        """
        # Initialize database tables
        init_db(self.engine)
        logger.info("Sync started", extra={"step": "db-sync"})

        report = SyncReport()

        with get_session(self.engine) as session:
            agency_repo = AgencyRepository(session)
            asset_repo = AssetRepository(session)
            workflow_repo = WorkflowRepository(session)
            sync_log_repo = SyncLogRepository(session)

            # Build lookup for resolved names -> IDs
            agency_id_map: dict[str, int] = {}
            asset_id_map: dict[str, int] = {}

            # Step 1: Sync agencies
            for data in self._load_yaml_files("agencies"):
                source_file = data.pop("_source_file", "unknown")
                try:
                    agency = Agency.model_validate(data)
                    agency_model, _ = agency_repo.upsert(
                        name=agency.metadata.name,
                        full_name=agency.spec.full_name,
                        base_url=agency.spec.base_url,
                        description=agency.spec.description,
                        labels=agency.metadata.labels,
                    )
                    agency_id_map[agency.metadata.name] = agency_model.id
                    report.agencies_synced += 1
                    sync_log_repo.log_sync("agency", agency.metadata.name, "success")
                    logger.info("Agency synced", extra={"step": "db-sync", "asset": agency.metadata.name})

                except ValidationError as e:
                    error_msg = f"Validation error in {source_file}: {e}"
                    report.agencies_errors += 1
                    report.errors.append(
                        SyncError("agency", source_file, error_msg)
                    )
                    sync_log_repo.log_sync("agency", source_file, "error", error_msg)
                    logger.error("Agency sync failed", extra={"step": "db-sync", "asset": source_file})

                except Exception as e:
                    error_msg = f"Error syncing from {source_file}: {e}"
                    report.agencies_errors += 1
                    report.errors.append(
                        SyncError("agency", source_file, error_msg)
                    )
                    sync_log_repo.log_sync("agency", source_file, "error", error_msg)
                    logger.error("Agency sync failed", extra={"step": "db-sync", "asset": source_file})

            # Load existing agencies that were already in DB
            for agency_model in agency_repo.list_all():
                if agency_model.name not in agency_id_map:
                    agency_id_map[agency_model.name] = agency_model.id

            # Step 2: Sync assets
            for data in self._load_yaml_files("assets"):
                source_file = data.pop("_source_file", "unknown")
                try:
                    asset = Asset.model_validate(data)

                    # Resolve agency reference
                    agency_id = agency_id_map.get(asset.spec.agency_ref)
                    if agency_id is None:
                        error_msg = (
                            f"Unknown agency_ref '{asset.spec.agency_ref}' "
                            f"in {source_file}"
                        )
                        report.assets_errors += 1
                        report.errors.append(
                            SyncError("asset", asset.metadata.name, error_msg)
                        )
                        sync_log_repo.log_sync(
                            "asset", asset.metadata.name, "error", error_msg
                        )
                        logger.error(error_msg, extra={"step": "db-sync", "asset": asset.metadata.name})
                        continue

                    # Serialize acquisition config to dict
                    acquisition_dict = self._serialize_acquisition_config(asset)

                    asset_model, _ = asset_repo.upsert(
                        name=asset.metadata.name,
                        agency_id=agency_id,
                        acquisition_config=acquisition_dict,
                        description=asset.spec.description,
                        labels=asset.metadata.labels,
                    )
                    asset_id_map[asset.metadata.name] = asset_model.id
                    report.assets_synced += 1
                    sync_log_repo.log_sync("asset", asset.metadata.name, "success")
                    logger.info("Asset synced", extra={"step": "db-sync", "asset": asset.metadata.name})

                except ValidationError as e:
                    error_msg = f"Validation error in {source_file}: {e}"
                    report.assets_errors += 1
                    report.errors.append(
                        SyncError("asset", source_file, error_msg)
                    )
                    sync_log_repo.log_sync("asset", source_file, "error", error_msg)
                    logger.error(error_msg, extra={"step": "db-sync", "asset": source_file})

                except Exception as e:
                    error_msg = f"Error syncing from {source_file}: {e}"
                    report.assets_errors += 1
                    report.errors.append(
                        SyncError("asset", source_file, error_msg)
                    )
                    sync_log_repo.log_sync("asset", source_file, "error", error_msg)
                    logger.error(error_msg, extra={"step": "db-sync", "asset": source_file})

            # Load existing assets that were already in DB
            for asset_model in asset_repo.list_all():
                if asset_model.name not in asset_id_map:
                    asset_id_map[asset_model.name] = asset_model.id

            # Step 3: Sync workflows
            for data in self._load_yaml_files("workflows"):
                source_file = data.pop("_source_file", "unknown")
                try:
                    workflow = Workflow.model_validate(data)

                    # Resolve asset reference
                    asset_id = asset_id_map.get(workflow.spec.asset_ref)
                    if asset_id is None:
                        error_msg = (
                            f"Unknown asset_ref '{workflow.spec.asset_ref}' "
                            f"in {source_file}"
                        )
                        report.workflows_errors += 1
                        report.errors.append(
                            SyncError("workflow", workflow.metadata.name, error_msg)
                        )
                        sync_log_repo.log_sync(
                            "workflow", workflow.metadata.name, "error", error_msg
                        )
                        logger.error(error_msg, extra={"step": "db-sync", "workflow": workflow.metadata.name})
                        continue

                    # Serialize steps to list of dicts
                    steps_list = self._serialize_steps(workflow)

                    _, _ = workflow_repo.upsert(
                        name=workflow.metadata.name,
                        asset_id=asset_id,
                        steps=steps_list,
                    )
                    report.workflows_synced += 1
                    sync_log_repo.log_sync("workflow", workflow.metadata.name, "success")
                    logger.info("Workflow synced", extra={"step": "db-sync", "workflow": workflow.metadata.name})

                except ValidationError as e:
                    error_msg = f"Validation error in {source_file}: {e}"
                    report.workflows_errors += 1
                    report.errors.append(
                        SyncError("workflow", source_file, error_msg)
                    )
                    sync_log_repo.log_sync("workflow", source_file, "error", error_msg)
                    logger.error(error_msg, extra={"step": "db-sync", "workflow": source_file})

                except Exception as e:
                    error_msg = f"Error syncing from {source_file}: {e}"
                    report.workflows_errors += 1
                    report.errors.append(
                        SyncError("workflow", source_file, error_msg)
                    )
                    sync_log_repo.log_sync("workflow", source_file, "error", error_msg)
                    logger.error(error_msg, extra={"step": "db-sync", "workflow": source_file})

            # Step 4: Sync MDA standard/1.0 manifests
            self._sync_mda_manifests(
                agency_repo, asset_repo, workflow_repo, sync_log_repo,
                agency_id_map, asset_id_map, report,
            )

        if report.success:
            logger.info(
                "Sync completed",
                extra={
                    "step": "db-sync",
                    "agencies": report.agencies_synced,
                    "assets": report.assets_synced,
                    "workflows": report.workflows_synced,
                },
            )
        else:
            logger.warning(
                "Sync completed with errors",
                extra={
                    "step": "db-sync",
                    "synced": report.total_synced,
                    "errors": report.total_errors,
                },
            )

        return report

    def _load_yaml_files(self, subdir: str) -> list[dict[str, Any]]:
        """Load all YAML files from a subdirectory.

        Mirrors the loading logic from Registry class.
        """
        import yaml

        path = self.manifests_path / subdir
        if not path.exists():
            return []

        results: list[dict[str, Any]] = []
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

    def _sync_mda_manifests(
        self,
        agency_repo: AgencyRepository,
        asset_repo: AssetRepository,
        workflow_repo: WorkflowRepository,
        sync_log_repo: SyncLogRepository,
        agency_id_map: dict[str, int],
        asset_id_map: dict[str, int],
        report: SyncReport,
    ) -> None:
        """Sync standard/1.0 (MDA) manifests to database.

        Scans the 'mda' subdirectory for manifests with a 'schema' field.
        For each standard/1.0 manifest, extracts agency, asset, and workflow
        info and upserts into the same tables as legacy manifests.
        """
        import yaml

        mda_path = self.manifests_path / "mda"
        if not mda_path.exists():
            return

        for yaml_file in list(mda_path.rglob("*.yaml")) + list(mda_path.rglob("*.yml")):
            try:
                with open(yaml_file) as f:
                    data = yaml.safe_load(f)

                if not data or not isinstance(data, dict):
                    continue

                # Only process standard/1.0 manifests
                schema = data.get("schema")
                if schema != "standard/1.0":
                    continue

                identity = data.get("identity", {})
                evolution = data.get("evolution", {})
                steps = data.get("steps", [])

                agency_name = identity.get("agency", "")
                asset_name = identity.get("name", "")
                workflow_name = evolution.get("manifest_id", asset_name)

                if not agency_name or not asset_name:
                    continue

                # Ensure agency exists (upsert with minimal info)
                if agency_name not in agency_id_map:
                    agency_model, _ = agency_repo.upsert(
                        name=agency_name,
                        full_name=agency_name.replace("-", " ").title(),
                        base_url="",
                        description=f"Agency from MDA manifest: {identity.get('domain', '')}",
                        labels={"domain": identity.get("domain", ""), "layer": identity.get("layer", "")},
                    )
                    agency_id_map[agency_name] = agency_model.id
                    report.agencies_synced += 1
                    sync_log_repo.log_sync("agency", agency_name, "success")

                agency_id = agency_id_map[agency_name]

                # Upsert asset (with step-derived acquisition config)
                acquisition_config = {
                    "type": "mda",
                    "format": "standard/1.0",
                    "provider": evolution.get("provider", ""),
                    "engine": evolution.get("engine", ""),
                }

                asset_model, _ = asset_repo.upsert(
                    name=asset_name,
                    agency_id=agency_id,
                    acquisition_config=acquisition_config,
                    description=f"MDA asset from {identity.get('layer', '')}:{identity.get('domain', '')}",
                    labels={"layer": identity.get("layer", ""), "domain": identity.get("domain", "")},
                )
                asset_id_map[asset_name] = asset_model.id
                report.assets_synced += 1
                sync_log_repo.log_sync("asset", asset_name, "success")

                # Upsert workflow
                steps_list = [
                    {
                        "name": s.get("step", "unnamed"),
                        "type": s.get("component", {}).get("path", "").split("/")[0],
                        "config": s.get("component", {}).get("params", {}),
                        "dependsOn": [],
                    }
                    for s in steps
                ]

                workflow_repo.upsert(
                    name=workflow_name,
                    asset_id=asset_model.id,
                    steps=steps_list,
                )
                report.workflows_synced += 1
                sync_log_repo.log_sync("workflow", workflow_name, "success")

            except Exception as e:
                error_msg = f"Error syncing MDA manifest {yaml_file}: {e}"
                report.workflows_errors += 1
                report.errors.append(SyncError("mda_manifest", str(yaml_file), error_msg))
                sync_log_repo.log_sync("mda_manifest", str(yaml_file), "error", error_msg)
                logger.error(error_msg, extra={"step": "db-sync"})

    def _serialize_acquisition_config(self, asset: Asset) -> dict[str, Any]:
        """Serialize asset acquisition config to a dictionary."""
        acq = asset.spec.acquisition
        result: dict[str, Any] = {
            "type": acq.type.value,
            "format": acq.format,
        }

        if acq.schedule:
            result["schedule"] = acq.schedule

        # Serialize source based on type
        source = acq.source
        if hasattr(source, "url") and source.url:
            # HttpSource with static URL
            result["source"] = {
                "url": source.url,
                "headers": getattr(source, "headers", {}),
            }
        elif hasattr(source, "temporal") and source.temporal:
            # HttpSource with temporal config
            result["source"] = {
                "temporal": {
                    "pattern": source.temporal.pattern.value,
                    "fiscalYearStartMonth": source.temporal.fiscal_year_start_month,
                    "urlTemplate": source.temporal.url_template,
                },
                "headers": getattr(source, "headers", {}),
            }
        elif hasattr(source, "base_url"):
            # ApiSource
            result["source"] = {
                "base_url": source.base_url,
                "endpoint": source.endpoint,
                "method": source.method,
                "params": source.params,
                "headers": source.headers,
            }
            if source.auth:
                result["source"]["auth"] = {
                    "type": source.auth.type.value,
                    "key_env_var": source.auth.key_env_var,
                    "header_name": source.auth.header_name,
                }

        return result

    def _serialize_steps(self, workflow: Workflow) -> list[dict[str, Any]]:
        """Serialize workflow steps to list of dictionaries."""
        return [
            {
                "name": step.name,
                "type": step.type,
                "config": step.config,
                "dependsOn": step.depends_on,
            }
            for step in workflow.spec.steps
        ]
