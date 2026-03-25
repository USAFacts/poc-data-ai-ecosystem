"""Weaviate status API routes for detailed collection and schema information."""

import os
from logging import getLogger
from typing import Any

from fastapi import APIRouter, HTTPException

logger = getLogger(__name__)

router = APIRouter()


@router.get("/status", summary="Weaviate Detailed Status")
def weaviate_status() -> dict[str, Any]:
    """Return detailed Weaviate status including connection info, collections, and schema details."""
    try:
        from src.services.weaviate_client import get_client

        client = get_client()

        host = os.getenv("WEAVIATE_HOST", "localhost")
        port = int(os.getenv("WEAVIATE_PORT", "8080"))
        grpc_port = int(os.getenv("WEAVIATE_GRPC_PORT", "50051"))

        # List all collections
        all_collections = client.collections.list_all()

        collections_info = []
        total_objects = 0

        for name, config in all_collections.items():
            collection = client.collections.get(name)

            # Get object count
            agg = collection.aggregate.over_all(total_count=True)
            count = agg.total_count or 0
            total_objects += count

            # Get schema properties
            properties = []
            for prop in config.properties:
                prop_info: dict[str, Any] = {
                    "name": prop.name,
                    "data_type": str(prop.data_type),
                }
                if hasattr(prop, "tokenization") and prop.tokenization is not None:
                    prop_info["tokenization"] = str(prop.tokenization)
                properties.append(prop_info)

            collections_info.append({
                "name": name,
                "object_count": count,
                "properties": properties,
            })

        return {
            "status": "connected",
            "connection": {
                "host": host,
                "port": port,
                "grpc_port": grpc_port,
            },
            "collections": collections_info,
            "total_objects": total_objects,
        }

    except Exception as e:
        logger.error(f"Weaviate status check failed: {e}")
        raise HTTPException(status_code=503, detail=f"Weaviate unavailable: {str(e)}")
