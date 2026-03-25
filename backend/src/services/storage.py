"""MinIO storage service for accessing processed data."""

import json
import os
from typing import Any

from minio import Minio
from minio.error import S3Error

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "gov-data-lake")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"


class StorageService:
    """Service for accessing MinIO storage."""

    def __init__(self) -> None:
        """Initialize MinIO client."""
        self.client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE,
        )
        self.bucket = MINIO_BUCKET

    def _ensure_bucket_exists(self) -> bool:
        """Check if the bucket exists."""
        try:
            return self.client.bucket_exists(self.bucket)
        except S3Error:
            return False

    def list_objects(self, prefix: str = "", recursive: bool = True) -> list[str]:
        """List objects in the bucket with optional prefix."""
        if not self._ensure_bucket_exists():
            return []

        objects = self.client.list_objects(
            self.bucket,
            prefix=prefix,
            recursive=recursive,
        )
        return [obj.object_name for obj in objects]

    def list_objects_with_metadata(
        self, prefix: str = "", recursive: bool = True
    ) -> list[dict[str, Any]]:
        """List objects with metadata including last_modified timestamp.

        Returns:
            List of dicts with 'name', 'last_modified', and 'size' keys
        """
        if not self._ensure_bucket_exists():
            return []

        objects = self.client.list_objects(
            self.bucket,
            prefix=prefix,
            recursive=recursive,
        )
        return [
            {
                "name": obj.object_name,
                "last_modified": obj.last_modified,
                "size": obj.size,
            }
            for obj in objects
        ]

    def get_object(self, object_name: str) -> bytes | None:
        """Get an object's content."""
        if not self._ensure_bucket_exists():
            return None

        try:
            response = self.client.get_object(self.bucket, object_name)
            return response.read()
        except S3Error:
            return None
        finally:
            if "response" in locals():
                response.close()
                response.release_conn()

    def get_json_object(self, object_name: str) -> dict[str, Any] | None:
        """Get an object and parse as JSON."""
        content = self.get_object(object_name)
        if content is None:
            return None

        try:
            return json.loads(content.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None

    def list_enrichment_files(self, workflow: str | None = None) -> list[str]:
        """List enrichment files, optionally filtered by workflow."""
        prefix = "enrichment-zone/"
        if workflow:
            prefix = f"enrichment-zone/{workflow}/"
        return self.list_objects(prefix)

# Singleton instance
_storage_service: StorageService | None = None


def get_storage_service() -> StorageService:
    """Get storage service singleton."""
    global _storage_service
    if _storage_service is None:
        _storage_service = StorageService()
    return _storage_service
