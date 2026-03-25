"""MinIO explorer for browsing the data lake.

Provides a high-level interface for discovering and loading data
from the MinIO-based data lake.
"""

import json
import os
from dataclasses import dataclass
from typing import Any

import pandas as pd
from minio import Minio
from minio.error import S3Error


# Storage zone constants (matching ingestion-processing naming)
LANDING_ZONE = "landing-zone"
PARSED_ZONE = "parsed-zone"
ENRICHMENT_ZONE = "enrichment-zone"
READY_ZONE = "ready-zone"

ZONES = [LANDING_ZONE, PARSED_ZONE, ENRICHMENT_ZONE, READY_ZONE]


class ExplorerError(Exception):
    """Error during data exploration."""

    pass


@dataclass
class AssetInfo:
    """Information about an asset in the data lake."""

    agency: str
    asset: str
    zone: str
    versions: list[str]
    latest_version: str | None

    @property
    def path(self) -> str:
        """Return the asset path as agency/asset."""
        return f"{self.agency}/{self.asset}"


class MinioExplorer:
    """Explorer for browsing the MinIO data lake.

    Provides methods to list zones, agencies, assets, and load documents/tables.

    Example:
        >>> explorer = MinioExplorer()
        >>> zones = explorer.list_zones()
        >>> agencies = explorer.list_agencies("parsed-zone")
        >>> assets = explorer.list_assets("uscis", "parsed-zone")
        >>> doc = explorer.load_document("uscis", "quarterly-forms")
        >>> df = explorer.load_table(doc, table_index=0)
    """

    def __init__(
        self,
        endpoint: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        bucket: str | None = None,
        secure: bool | None = None,
    ) -> None:
        """Initialize the explorer.

        Args:
            endpoint: MinIO endpoint (default: from MINIO_ENDPOINT env var)
            access_key: Access key (default: from MINIO_ACCESS_KEY env var)
            secret_key: Secret key (default: from MINIO_SECRET_KEY env var)
            bucket: Default bucket name (default: from MINIO_BUCKET env var)
            secure: Use HTTPS (default: from MINIO_SECURE env var)
        """
        self.endpoint = endpoint or os.getenv("MINIO_ENDPOINT", "localhost:9000")
        self.access_key = access_key or os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.secret_key = secret_key or os.getenv("MINIO_SECRET_KEY", "minioadmin")
        self.bucket = bucket or os.getenv("MINIO_BUCKET", "gov-data-lake")

        secure_env = os.getenv("MINIO_SECURE", "false")
        self.secure = secure if secure is not None else secure_env.lower() == "true"

        self._client: Minio | None = None

    @property
    def client(self) -> Minio:
        """Get or create MinIO client."""
        if self._client is None:
            self._client = Minio(
                self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=self.secure,
            )
        return self._client

    def list_zones(self) -> list[str]:
        """List available storage zones.

        Returns:
            List of zone names that have data
        """
        available_zones = []
        for zone in ZONES:
            try:
                # Check if zone has any objects (just need first one)
                for obj in self.client.list_objects(self.bucket, prefix=f"{zone}/"):
                    available_zones.append(zone)
                    break  # Found at least one object, move to next zone
            except S3Error:
                continue
        return available_zones

    def list_agencies(self, zone: str = PARSED_ZONE) -> list[str]:
        """List agencies with data in a zone.

        Args:
            zone: Storage zone to query (default: parsed-zone)

        Returns:
            Sorted list of agency names
        """
        try:
            objects = self.client.list_objects(self.bucket, prefix=f"{zone}/", recursive=True)
            agencies = set()
            for obj in objects:
                parts = obj.object_name.split("/")
                if len(parts) >= 2:
                    agencies.add(parts[1])
            return sorted(agencies)
        except S3Error as e:
            raise ExplorerError(f"Failed to list agencies: {e}") from e

    def list_assets(self, agency: str, zone: str = PARSED_ZONE) -> list[AssetInfo]:
        """List assets for an agency in a zone.

        Args:
            agency: Agency name
            zone: Storage zone to query (default: parsed-zone)

        Returns:
            List of AssetInfo objects
        """
        try:
            prefix = f"{zone}/{agency}/"
            objects = self.client.list_objects(self.bucket, prefix=prefix, recursive=True)

            # Group by asset
            assets: dict[str, set[str]] = {}
            for obj in objects:
                parts = obj.object_name.split("/")
                # Path: zone/agency/asset/datestamp/timestamp/filename
                if len(parts) >= 5:
                    asset = parts[2]
                    version = f"{parts[3]}/{parts[4]}"
                    if asset not in assets:
                        assets[asset] = set()
                    assets[asset].add(version)

            # Build AssetInfo list
            result = []
            for asset, versions in assets.items():
                sorted_versions = sorted(versions, reverse=True)
                result.append(
                    AssetInfo(
                        agency=agency,
                        asset=asset,
                        zone=zone,
                        versions=sorted_versions,
                        latest_version=sorted_versions[0] if sorted_versions else None,
                    )
                )
            return sorted(result, key=lambda a: a.asset)

        except S3Error as e:
            raise ExplorerError(f"Failed to list assets: {e}") from e

    def load_document(
        self,
        agency: str,
        asset: str,
        zone: str = PARSED_ZONE,
        version: str | None = None,
    ) -> dict[str, Any]:
        """Load a parsed document from the data lake.

        Args:
            agency: Agency name
            asset: Asset name
            zone: Storage zone (default: parsed-zone)
            version: Specific version (datestamp/timestamp), or None for latest

        Returns:
            Parsed document dictionary

        Raises:
            ExplorerError: If document cannot be loaded
        """
        try:
            # Find the version
            if version is None:
                assets = self.list_assets(agency, zone)
                matching = [a for a in assets if a.asset == asset]
                if not matching:
                    raise ExplorerError(f"Asset not found: {agency}/{asset} in {zone}")
                version = matching[0].latest_version
                if not version:
                    raise ExplorerError(f"No versions found for: {agency}/{asset}")

            # Find the JSON document
            prefix = f"{zone}/{agency}/{asset}/{version}/"
            objects = list(self.client.list_objects(self.bucket, prefix=prefix))

            json_file = None
            for obj in objects:
                if obj.object_name.endswith(".json") and not obj.object_name.endswith(
                    "_metadata.json"
                ):
                    json_file = obj.object_name
                    break

            if not json_file:
                raise ExplorerError(f"No document found at: {prefix}")

            # Load the document
            response = self.client.get_object(self.bucket, json_file)
            try:
                content = response.read().decode("utf-8")
                document = json.loads(content)
                # Add source info
                document["_source"] = {
                    "agency": agency,
                    "asset": asset,
                    "zone": zone,
                    "version": version,
                    "path": json_file,
                }
                return document
            finally:
                response.close()
                response.release_conn()

        except S3Error as e:
            raise ExplorerError(f"Failed to load document: {e}") from e

    def list_tables(self, document: dict[str, Any]) -> list[dict[str, Any]]:
        """List tables in a document with summary info.

        Args:
            document: Parsed document dictionary

        Returns:
            List of table info dictionaries with title, column count, row count
        """
        content = document.get("content", {})
        tables = content.get("tables", [])

        result = []
        for i, table in enumerate(tables):
            headers = table.get("headers", [])
            rows = table.get("rows", [])
            result.append(
                {
                    "index": i,
                    "title": table.get("title", f"Table {i}"),
                    "columns": len(headers),
                    "rows": len(rows),
                    "headers": headers,
                }
            )
        return result

    def load_table(
        self,
        document: dict[str, Any],
        table_index: int = 0,
    ) -> pd.DataFrame:
        """Load a table from a document as a pandas DataFrame.

        Args:
            document: Parsed document dictionary
            table_index: Index of table to load (default: 0)

        Returns:
            pandas DataFrame with the table data
        """
        content = document.get("content", {})
        tables = content.get("tables", [])

        if table_index < 0 or table_index >= len(tables):
            raise ExplorerError(
                f"Table index {table_index} out of range. Document has {len(tables)} tables."
            )

        table = tables[table_index]
        headers = table.get("headers", [])
        rows = table.get("rows", [])

        # Handle case where headers are missing
        if not headers and rows:
            headers = [f"column_{i}" for i in range(len(rows[0]))]

        return pd.DataFrame(rows, columns=headers)

    def get_enrichment(
        self,
        agency: str,
        asset: str,
        version: str | None = None,
    ) -> dict[str, Any] | None:
        """Load enrichment data for an asset if available.

        Args:
            agency: Agency name
            asset: Asset name
            version: Specific version, or None for latest

        Returns:
            Enrichment dictionary or None if not found
        """
        try:
            return self.load_document(agency, asset, zone=ENRICHMENT_ZONE, version=version)
        except ExplorerError:
            return None
