"""Convenience functions for loading documents and tables.

These are shorthand functions that create an explorer and load data
in a single call.
"""

from typing import Any

import pandas as pd

from analysis_helpers.explorer import MinioExplorer, PARSED_ZONE


def load_document(
    agency: str,
    asset: str,
    zone: str = PARSED_ZONE,
    version: str | None = None,
    **explorer_kwargs: Any,
) -> dict[str, Any]:
    """Load a parsed document from the data lake.

    This is a convenience function that creates an explorer and loads
    the document in a single call.

    Args:
        agency: Agency name (e.g., "uscis")
        asset: Asset name (e.g., "quarterly-forms")
        zone: Storage zone (default: parsed-zone)
        version: Specific version (datestamp/timestamp), or None for latest
        **explorer_kwargs: Additional arguments passed to MinioExplorer

    Returns:
        Parsed document dictionary

    Example:
        >>> doc = load_document("uscis", "quarterly-forms")
        >>> print(doc["metadata"]["title"])
    """
    explorer = MinioExplorer(**explorer_kwargs)
    return explorer.load_document(agency, asset, zone, version)


def load_table(
    agency: str,
    asset: str,
    table_index: int = 0,
    zone: str = PARSED_ZONE,
    version: str | None = None,
    **explorer_kwargs: Any,
) -> pd.DataFrame:
    """Load a table from a document as a pandas DataFrame.

    This is a convenience function that creates an explorer, loads
    the document, and extracts the table in a single call.

    Args:
        agency: Agency name (e.g., "uscis")
        asset: Asset name (e.g., "quarterly-forms")
        table_index: Index of table to load (default: 0)
        zone: Storage zone (default: parsed-zone)
        version: Specific version (datestamp/timestamp), or None for latest
        **explorer_kwargs: Additional arguments passed to MinioExplorer

    Returns:
        pandas DataFrame with the table data

    Example:
        >>> df = load_table("uscis", "quarterly-forms", table_index=0)
        >>> df.head()
    """
    explorer = MinioExplorer(**explorer_kwargs)
    document = explorer.load_document(agency, asset, zone, version)
    return explorer.load_table(document, table_index)
