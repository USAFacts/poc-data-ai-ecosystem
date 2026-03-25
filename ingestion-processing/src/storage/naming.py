"""Object naming conventions for MinIO storage."""

from datetime import datetime, timezone

# Zone prefixes for different pipeline stages
LANDING_ZONE = "landing-zone"            # Raw acquired data
PARSED_ZONE = "parsed-zone"              # Parsed/extracted structured data
ENRICHMENT_ZONE = "enrichment-zone"      # Enriched data with semantic context for RAG
CHUNK_ZONE = "chunk-zone"                # Hierarchical chunks for granular retrieval
READY_ZONE = "ready-zone"                # Post-validation data


def build_object_path(
    agency_name: str,
    asset_name: str,
    timestamp: datetime | None = None,
    extension: str = "csv",
    zone: str = LANDING_ZONE,
) -> str:
    """Generate object path following naming convention.

    Pattern: {zone}/{agency}/{asset}/{datestamp}/{timestring}/{asset_name}.{extension}

    Args:
        agency_name: Name of the agency
        asset_name: Name of the asset (also used as filename)
        timestamp: Timestamp for this acquisition (defaults to now)
        extension: File extension (default: "csv")
        zone: Storage zone prefix (default: landing-zone)

    Returns:
        Object path string
    """
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)

    datestamp = timestamp.strftime("%Y-%m-%d")
    time_str = timestamp.strftime("%H%M%S")
    return f"{zone}/{agency_name}/{asset_name}/{datestamp}/{time_str}/{asset_name}.{extension}"


def build_metadata_path(
    agency_name: str,
    asset_name: str,
    timestamp: datetime | None = None,
    zone: str = LANDING_ZONE,
) -> str:
    """Generate metadata object path.

    Args:
        agency_name: Name of the agency
        asset_name: Name of the asset
        timestamp: Timestamp for this acquisition (defaults to now)
        zone: Storage zone prefix (default: landing-zone)

    Returns:
        Metadata object path string
    """
    if timestamp is None:
        timestamp = datetime.now(timezone.utc)

    datestamp = timestamp.strftime("%Y-%m-%d")
    time_str = timestamp.strftime("%H%M%S")
    return f"{zone}/{agency_name}/{asset_name}/{datestamp}/{time_str}/_metadata.json"


def parse_object_path(path: str) -> dict[str, str]:
    """Parse an object path into its components.

    Args:
        path: Object path to parse

    Returns:
        Dictionary with zone, agency, asset, datestamp, timestamp, and filename components

    Raises:
        ValueError: If path doesn't match expected format
    """
    parts = path.split("/")
    if len(parts) < 6:
        raise ValueError(f"Invalid object path: {path}")

    return {
        "zone": parts[0],
        "agency": parts[1],
        "asset": parts[2],
        "datestamp": parts[3],
        "timestamp": parts[4],
        "filename": "/".join(parts[5:]),
    }
