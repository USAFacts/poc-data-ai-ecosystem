"""Acquisition step implementation."""

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from control.models import AcquisitionType, HttpSource, ApiSource
from runtime.context import ExecutionContext
from steps.base import Step, StepResult, StepStatus
from steps.acquisition.connectors import (
    ConnectorError,
    HttpConnector,
    ApiConnector,
    get_connector,
)
from storage.naming import LANDING_ZONE


class AcquisitionStep(Step):
    """Step that acquires data from external sources.

    Uses pluggable connectors to support different data source types:
    - HTTP/HTTPS downloads (static URLs and temporal templates)
    - REST APIs with authentication
    - Future: SFTP, S3, databases, etc.

    Data is stored in the landing-zone of MinIO storage.
    """

    step_type = "acquisition"

    def __init__(self, name: str, config: dict[str, Any] | None = None) -> None:
        """Initialize acquisition step.

        Args:
            name: Step name
            config: Step configuration (optional overrides)
        """
        super().__init__(name, config)
        # Connectors are instantiated per-fetch to allow configuration
        self._connectors: dict[str, Any] = {}

    def _get_connector(self, connector_type: str):
        """Get or create a connector instance.

        Args:
            connector_type: Type of connector (http, api, etc.)

        Returns:
            Connector instance
        """
        if connector_type not in self._connectors:
            connector = get_connector(connector_type)
            if connector is None:
                raise ValueError(f"Unknown connector type: {connector_type}")
            self._connectors[connector_type] = connector
        return self._connectors[connector_type]

    def validate_config(self) -> list[str]:
        """Validate step configuration.

        Returns:
            List of validation error messages
        """
        errors = []
        # Config validation is minimal for acquisition step
        # Most validation happens at the asset level via connectors
        return errors

    def execute(self, context: ExecutionContext) -> StepResult:
        """Execute the acquisition step.

        Fetches data using the appropriate connector and stores it
        in the landing-zone of MinIO storage.

        Args:
            context: Execution context with asset, storage, etc.

        Returns:
            StepResult with status and output
        """
        started_at = datetime.now(timezone.utc)

        try:
            # Get acquisition config from asset
            acquisition_config = context.asset.spec.acquisition
            connector_type = acquisition_config.type.value

            # Get the appropriate connector
            connector = self._get_connector(connector_type)

            # Fetch data using connector
            if acquisition_config.type == AcquisitionType.HTTP:
                source = acquisition_config.source
                if not isinstance(source, HttpSource):
                    raise ValueError("HTTP acquisition requires HttpSource")
                result = connector.fetch(source)

            elif acquisition_config.type == AcquisitionType.API:
                source = acquisition_config.source
                if not isinstance(source, ApiSource):
                    raise ValueError("API acquisition requires ApiSource")
                result = connector.fetch(source)

            else:
                raise ValueError(f"Unknown acquisition type: {acquisition_config.type}")

            # Calculate checksum
            checksum = hashlib.sha256(result.data).hexdigest()

            # Calculate duration
            completed_at = datetime.now(timezone.utc)
            duration_ms = int((completed_at - started_at).total_seconds() * 1000)

            # Store to MinIO landing zone
            object_path = self._store_data(
                context=context,
                data=result.data,
                format=acquisition_config.format,
                source_url=result.source_url,
                checksum=checksum,
                fetch_metadata=result.metadata,
                duration_ms=duration_ms,
            )

            # Build output
            output = {
                "object_path": object_path,
                "bytes_stored": result.size,
                "source_url": result.source_url,
                "format": acquisition_config.format,
                "checksum": f"sha256:{checksum}",
                "connector_type": connector_type,
                "zone": LANDING_ZONE,
            }

            return StepResult(
                status=StepStatus.SUCCESS,
                started_at=started_at,
                completed_at=completed_at,
                output=output,
            )

        except ConnectorError as e:
            return StepResult(
                status=StepStatus.FAILED,
                started_at=started_at,
                completed_at=datetime.now(timezone.utc),
                error=str(e),
            )
        except Exception as e:
            return StepResult(
                status=StepStatus.FAILED,
                started_at=started_at,
                completed_at=datetime.now(timezone.utc),
                error=f"Unexpected error: {e}",
            )

    def _store_data(
        self,
        context: ExecutionContext,
        data: bytes,
        format: str,
        source_url: str,
        checksum: str,
        fetch_metadata: dict[str, str],
        duration_ms: int,
    ) -> str:
        """Store acquired data to MinIO landing zone.

        Args:
            context: Execution context
            data: Raw data bytes
            format: Data format (csv, xlsx, json, etc.)
            source_url: Source URL the data was fetched from
            checksum: SHA256 checksum of data
            fetch_metadata: Additional metadata from connector
            duration_ms: Acquisition duration in milliseconds

        Returns:
            Object path where data was stored
        """
        storage = context.storage

        # Build object path using naming conventions
        agency_name = context.agency.metadata.name
        asset_name = context.asset.metadata.name
        execution_time = context.execution_time

        # Path format: {zone}/{agency}/{asset}/{datestamp}/{timestamp}/{asset_name}.{format}
        datestamp = execution_time.strftime("%Y-%m-%d")
        timestamp = execution_time.strftime("%H%M%S")
        filename = f"{asset_name}.{format}"
        object_path = f"{LANDING_ZONE}/{agency_name}/{asset_name}/{datestamp}/{timestamp}/{filename}"

        # Build metadata
        metadata = {
            "acquired_at": execution_time.isoformat(),
            "source_url": source_url,
            "workflow": context.plan.workflow_name,
            "step": self.name,
            "run_id": context.master_utid or context.run_id,
            "checksum": f"sha256:{checksum}",
            "zone": LANDING_ZONE,
            "labels": json.dumps(context.asset.metadata.labels),
            "duration_ms": str(duration_ms),
            **{k: v for k, v in fetch_metadata.items() if v},  # Include non-empty metadata
        }

        # Upload to storage
        storage.put_object(
            object_name=object_path,
            data=data,
            content_type=self._get_content_type(format),
            metadata=metadata,
        )

        return object_path

    def _get_content_type(self, format: str) -> str:
        """Get content type for a format.

        Args:
            format: File format (csv, xlsx, json, etc.)

        Returns:
            MIME content type
        """
        content_types = {
            "csv": "text/csv",
            "json": "application/json",
            "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "xls": "application/vnd.ms-excel",
            "xml": "application/xml",
            "txt": "text/plain",
            "pdf": "application/pdf",
            "zip": "application/zip",
            "parquet": "application/octet-stream",
        }
        return content_types.get(format, "application/octet-stream")
