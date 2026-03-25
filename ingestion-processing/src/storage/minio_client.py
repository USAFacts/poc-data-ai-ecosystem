"""MinIO storage client wrapper."""

import hashlib
import io
import json
import os
from datetime import datetime, timezone
from typing import Any

from minio import Minio
from minio.error import S3Error

from storage.naming import build_object_path, build_metadata_path, LANDING_ZONE


class StorageError(Exception):
    """Error during storage operations."""

    pass


class MinioStorage:
    """MinIO storage client with naming conventions.

    Provides a simple interface for storing and retrieving data
    with automatic metadata management.
    """

    def __init__(
        self,
        endpoint: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        bucket: str | None = None,
        secure: bool | None = None,
    ) -> None:
        """Initialize MinIO client.

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

    def ensure_bucket(self) -> None:
        """Ensure the bucket exists, creating it if necessary."""
        try:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
        except S3Error as e:
            raise StorageError(f"Failed to ensure bucket exists: {e}") from e

    def put_object(
        self,
        object_name: str,
        data: bytes,
        content_type: str = "application/octet-stream",
        metadata: dict[str, str] | None = None,
    ) -> str:
        """Store an object directly with a given path.

        Args:
            object_name: Full object path in the bucket
            data: Raw data bytes to store
            content_type: MIME content type
            metadata: Optional metadata to attach

        Returns:
            Object path where data was stored
        """
        self.ensure_bucket()

        try:
            data_stream = io.BytesIO(data)
            self.client.put_object(
                self.bucket,
                object_name,
                data_stream,
                length=len(data),
                content_type=content_type,
                metadata=metadata,
            )
            return object_name

        except S3Error as e:
            raise StorageError(f"Failed to store object: {e}") from e

    def store_data(
        self,
        data: bytes,
        agency_name: str,
        asset_name: str,
        format: str = "csv",
        timestamp: datetime | None = None,
        metadata: dict[str, Any] | None = None,
        zone: str = LANDING_ZONE,
    ) -> str:
        """Store data with metadata using naming conventions.

        Path structure: {zone}/{agency}/{asset}/{datestamp}/{timestamp}/{asset_name}.{format}

        Args:
            data: Raw data bytes to store
            agency_name: Name of the agency
            asset_name: Name of the asset (also used as filename)
            format: Data format/extension
            timestamp: Timestamp for this acquisition
            metadata: Additional metadata to store
            zone: Storage zone prefix (default: landing-zone)

        Returns:
            Object path where data was stored
        """
        if timestamp is None:
            timestamp = datetime.now(timezone.utc)

        self.ensure_bucket()

        # Generate paths
        data_path = build_object_path(
            agency_name=agency_name,
            asset_name=asset_name,
            timestamp=timestamp,
            extension=format,
            zone=zone,
        )
        metadata_path = build_metadata_path(
            agency_name=agency_name,
            asset_name=asset_name,
            timestamp=timestamp,
            zone=zone,
        )

        # Calculate checksum
        checksum = hashlib.sha256(data).hexdigest()

        # Build full metadata
        full_metadata = {
            "acquired_at": timestamp.isoformat() + "Z",
            "checksum": f"sha256:{checksum}",
            "size_bytes": len(data),
            "format": format,
            "agency": agency_name,
            "asset": asset_name,
            **(metadata or {}),
        }

        try:
            # Store data
            data_stream = io.BytesIO(data)
            self.client.put_object(
                self.bucket,
                data_path,
                data_stream,
                length=len(data),
                content_type=self._get_content_type(format),
            )

            # Store metadata
            metadata_bytes = json.dumps(full_metadata, indent=2).encode("utf-8")
            metadata_stream = io.BytesIO(metadata_bytes)
            self.client.put_object(
                self.bucket,
                metadata_path,
                metadata_stream,
                length=len(metadata_bytes),
                content_type="application/json",
            )

            return data_path

        except S3Error as e:
            raise StorageError(f"Failed to store data: {e}") from e

    def get_object(self, object_path: str) -> bytes:
        """Retrieve data from storage.

        Args:
            object_path: Path to the object

        Returns:
            Raw data bytes
        """
        try:
            response = self.client.get_object(self.bucket, object_path)
            return response.read()
        except S3Error as e:
            raise StorageError(f"Failed to retrieve data: {e}") from e
        finally:
            if "response" in locals():
                response.close()
                response.release_conn()

    def get_metadata(
        self,
        agency_name: str,
        asset_name: str,
        datestamp: str,
        timestamp: str,
        zone: str = LANDING_ZONE,
    ) -> dict[str, Any]:
        """Retrieve metadata for an acquisition.

        Args:
            agency_name: Name of the agency
            asset_name: Name of the asset
            datestamp: Date string (format: YYYY-MM-DD)
            timestamp: Time string (format: HHMMSS)
            zone: Storage zone (default: landing-zone)

        Returns:
            Metadata dictionary
        """
        metadata_path = f"{zone}/{agency_name}/{asset_name}/{datestamp}/{timestamp}/_metadata.json"
        try:
            response = self.client.get_object(self.bucket, metadata_path)
            return json.loads(response.read().decode("utf-8"))
        except S3Error as e:
            raise StorageError(f"Failed to retrieve metadata: {e}") from e
        finally:
            if "response" in locals():
                response.close()
                response.release_conn()

    def list_assets(
        self, agency_name: str | None = None, zone: str = LANDING_ZONE
    ) -> list[str]:
        """List available assets, optionally filtered by agency.

        Args:
            agency_name: Optional agency name to filter by
            zone: Storage zone to list from (default: landing-zone)

        Returns:
            List of asset paths (agency/asset format)
        """
        try:
            if agency_name:
                prefix = f"{zone}/{agency_name}/"
            else:
                prefix = f"{zone}/"
            objects = self.client.list_objects(self.bucket, prefix=prefix, recursive=True)

            assets = set()
            for obj in objects:
                parts = obj.object_name.split("/")
                # Path: zone/agency/asset/datestamp/timestamp/filename
                if len(parts) >= 3:
                    assets.add(f"{parts[1]}/{parts[2]}")

            return sorted(assets)
        except S3Error as e:
            raise StorageError(f"Failed to list assets: {e}") from e

    def list_versions(
        self, agency_name: str, asset_name: str, zone: str = LANDING_ZONE
    ) -> list[str]:
        """List available versions for an asset.

        Args:
            agency_name: Name of the agency
            asset_name: Name of the asset
            zone: Storage zone to list from (default: landing-zone)

        Returns:
            List of version strings as 'datestamp/timestamp' (newest first)
        """
        try:
            prefix = f"{zone}/{agency_name}/{asset_name}/"
            objects = self.client.list_objects(self.bucket, prefix=prefix, recursive=True)

            versions = set()
            for obj in objects:
                parts = obj.object_name.split("/")
                # Path: zone/agency/asset/datestamp/timestamp/filename
                if len(parts) >= 6:
                    versions.add(f"{parts[3]}/{parts[4]}")

            return sorted(versions, reverse=True)
        except S3Error as e:
            raise StorageError(f"Failed to list versions: {e}") from e

    def object_exists(self, object_path: str) -> bool:
        """Check if an object exists.

        Args:
            object_path: Path to check

        Returns:
            True if object exists
        """
        try:
            self.client.stat_object(self.bucket, object_path)
            return True
        except S3Error:
            return False

    @staticmethod
    def _get_content_type(format: str) -> str:
        """Get MIME content type for a format."""
        content_types = {
            "csv": "text/csv",
            "json": "application/json",
            "xml": "application/xml",
            "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "xls": "application/vnd.ms-excel",
            "zip": "application/zip",
            "parquet": "application/octet-stream",
            "pdf": "application/pdf",
            "txt": "text/plain",
        }
        return content_types.get(format.lower(), "application/octet-stream")
