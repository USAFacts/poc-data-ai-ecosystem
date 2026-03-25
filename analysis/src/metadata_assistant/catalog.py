"""Catalog client for persisting metadata to MinIO.

Provides storage and retrieval of curated metadata in the
metadata-catalog bucket.
"""

import io
import json
import os
from datetime import datetime, timezone
from typing import Any

from minio import Minio
from minio.error import S3Error

from metadata_assistant.models import TableMetadata


CATALOG_BUCKET = "metadata-catalog"


class CatalogError(Exception):
    """Error during catalog operations."""

    pass


class CatalogClient:
    """Client for the metadata catalog.

    Stores and retrieves TableMetadata in MinIO with a structured
    path convention: {agency}/{asset}/{table_id}.json

    Example:
        >>> catalog = CatalogClient()
        >>> catalog.save(metadata)
        >>> loaded = catalog.load("uscis", "quarterly-forms", "table-0")
    """

    def __init__(
        self,
        endpoint: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        bucket: str | None = None,
        secure: bool | None = None,
    ) -> None:
        """Initialize the catalog client.

        Args:
            endpoint: MinIO endpoint (default: from MINIO_ENDPOINT env var)
            access_key: Access key (default: from MINIO_ACCESS_KEY env var)
            secret_key: Secret key (default: from MINIO_SECRET_KEY env var)
            bucket: Catalog bucket name (default: metadata-catalog)
            secure: Use HTTPS (default: from MINIO_SECURE env var)
        """
        self.endpoint = endpoint or os.getenv("MINIO_ENDPOINT", "localhost:9000")
        self.access_key = access_key or os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.secret_key = secret_key or os.getenv("MINIO_SECRET_KEY", "minioadmin")
        self.bucket = bucket or os.getenv("METADATA_CATALOG_BUCKET", CATALOG_BUCKET)

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

    def _ensure_bucket(self) -> None:
        """Ensure the catalog bucket exists."""
        try:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
        except S3Error as e:
            raise CatalogError(f"Failed to ensure bucket exists: {e}") from e

    def _build_path(self, agency: str, asset: str, table_id: str) -> str:
        """Build the object path for a table's metadata."""
        return f"{agency}/{asset}/{table_id}.json"

    def save(self, metadata: TableMetadata) -> str:
        """Save table metadata to the catalog.

        Args:
            metadata: TableMetadata to save

        Returns:
            Path where metadata was stored

        Raises:
            CatalogError: If save fails
        """
        self._ensure_bucket()

        # Update timestamp
        metadata.curated_at = datetime.now(timezone.utc)

        path = self._build_path(metadata.agency, metadata.asset, metadata.table_id)

        try:
            data = json.dumps(metadata.to_dict(), indent=2)
            data_bytes = data.encode("utf-8")
            data_stream = io.BytesIO(data_bytes)

            self.client.put_object(
                self.bucket,
                path,
                data_stream,
                length=len(data_bytes),
                content_type="application/json",
            )

            return path

        except S3Error as e:
            raise CatalogError(f"Failed to save metadata: {e}") from e

    def load(self, agency: str, asset: str, table_id: str) -> TableMetadata:
        """Load table metadata from the catalog.

        Args:
            agency: Agency name
            asset: Asset name
            table_id: Table identifier

        Returns:
            TableMetadata loaded from catalog

        Raises:
            CatalogError: If load fails or metadata not found
        """
        path = self._build_path(agency, asset, table_id)

        try:
            response = self.client.get_object(self.bucket, path)
            try:
                data = json.loads(response.read().decode("utf-8"))
                return TableMetadata.from_dict(data)
            finally:
                response.close()
                response.release_conn()

        except S3Error as e:
            if e.code == "NoSuchKey":
                raise CatalogError(f"Metadata not found: {path}") from e
            raise CatalogError(f"Failed to load metadata: {e}") from e

    def exists(self, agency: str, asset: str, table_id: str) -> bool:
        """Check if metadata exists in the catalog.

        Args:
            agency: Agency name
            asset: Asset name
            table_id: Table identifier

        Returns:
            True if metadata exists
        """
        path = self._build_path(agency, asset, table_id)
        try:
            self.client.stat_object(self.bucket, path)
            return True
        except S3Error:
            return False

    def delete(self, agency: str, asset: str, table_id: str) -> None:
        """Delete metadata from the catalog.

        Args:
            agency: Agency name
            asset: Asset name
            table_id: Table identifier

        Raises:
            CatalogError: If delete fails
        """
        path = self._build_path(agency, asset, table_id)
        try:
            self.client.remove_object(self.bucket, path)
        except S3Error as e:
            raise CatalogError(f"Failed to delete metadata: {e}") from e

    def list_agencies(self) -> list[str]:
        """List agencies with metadata in the catalog.

        Returns:
            List of agency names
        """
        try:
            objects = self.client.list_objects(self.bucket, recursive=True)
            agencies = set()
            for obj in objects:
                parts = obj.object_name.split("/")
                if len(parts) >= 1:
                    agencies.add(parts[0])
            return sorted(agencies)
        except S3Error as e:
            raise CatalogError(f"Failed to list agencies: {e}") from e

    def list_assets(self, agency: str) -> list[str]:
        """List assets with metadata for an agency.

        Args:
            agency: Agency name

        Returns:
            List of asset names
        """
        try:
            prefix = f"{agency}/"
            objects = self.client.list_objects(self.bucket, prefix=prefix, recursive=True)
            assets = set()
            for obj in objects:
                parts = obj.object_name.split("/")
                if len(parts) >= 2:
                    assets.add(parts[1])
            return sorted(assets)
        except S3Error as e:
            raise CatalogError(f"Failed to list assets: {e}") from e

    def list_tables(self, agency: str, asset: str) -> list[str]:
        """List table IDs with metadata for an asset.

        Args:
            agency: Agency name
            asset: Asset name

        Returns:
            List of table IDs
        """
        try:
            prefix = f"{agency}/{asset}/"
            objects = self.client.list_objects(self.bucket, prefix=prefix)
            tables = []
            for obj in objects:
                # Extract table ID from filename
                filename = obj.object_name.split("/")[-1]
                if filename.endswith(".json"):
                    tables.append(filename[:-5])  # Remove .json
            return sorted(tables)
        except S3Error as e:
            raise CatalogError(f"Failed to list tables: {e}") from e

    def load_all_for_asset(self, agency: str, asset: str) -> list[TableMetadata]:
        """Load all table metadata for an asset.

        Args:
            agency: Agency name
            asset: Asset name

        Returns:
            List of TableMetadata objects
        """
        table_ids = self.list_tables(agency, asset)
        return [self.load(agency, asset, tid) for tid in table_ids]
