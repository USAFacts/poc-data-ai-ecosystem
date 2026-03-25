"""Main MetadataAssistant class.

Provides a unified interface for all metadata curation operations
including suggestions, validation, and catalog persistence.
"""

from typing import Any

from metadata_assistant.models import (
    TableMetadata,
    ColumnMetadata,
    TableRelationship,
    ValidationResult,
)
from metadata_assistant.suggestions import (
    suggest_table_metadata,
    suggest_column_metadata,
    DEFAULT_MODEL,
)
from metadata_assistant.relationships import (
    infer_relationships,
    find_common_columns,
    find_semantic_matches,
)
from metadata_assistant.validation import validate_metadata, check_column_coverage
from metadata_assistant.catalog import CatalogClient


class MetadataAssistant:
    """Assistant for curating table and column metadata.

    Provides LLM-powered suggestions, validation, relationship inference,
    and catalog persistence.

    Example:
        >>> assistant = MetadataAssistant()
        >>> metadata = assistant.suggest_table_metadata(doc, table_index=0)
        >>> metadata.display_name = "Custom Name"  # Modify as needed
        >>> validation = assistant.validate_metadata(metadata)
        >>> if validation.is_valid:
        ...     assistant.save_to_catalog(metadata)
    """

    def __init__(
        self,
        model: str = DEFAULT_MODEL,
        catalog_client: CatalogClient | None = None,
    ) -> None:
        """Initialize the metadata assistant.

        Args:
            model: Claude model to use for suggestions
            catalog_client: Optional catalog client (created if not provided)
        """
        self.model = model
        self._catalog = catalog_client

    @property
    def catalog(self) -> CatalogClient:
        """Get or create catalog client."""
        if self._catalog is None:
            self._catalog = CatalogClient()
        return self._catalog

    def suggest_table_metadata(
        self,
        document: dict[str, Any],
        table_index: int,
    ) -> TableMetadata:
        """Generate metadata suggestions for a table using LLM.

        Analyzes the table structure and content to suggest:
        - Display name
        - Description
        - Data domain
        - Column metadata (names, types, descriptions)

        Args:
            document: Parsed document containing the table
            table_index: Index of the table (0-based)

        Returns:
            TableMetadata with suggested values

        Example:
            >>> doc = explorer.load_document("uscis", "quarterly-forms")
            >>> metadata = assistant.suggest_table_metadata(doc, 0)
            >>> print(metadata.display_name)
        """
        return suggest_table_metadata(document, table_index, self.model)

    def suggest_column_metadata(
        self,
        document: dict[str, Any],
        table_index: int,
        column_name: str,
    ) -> ColumnMetadata:
        """Generate metadata suggestions for a specific column.

        Provides more focused analysis for a single column when
        the table-level suggestions need refinement.

        Args:
            document: Parsed document containing the table
            table_index: Index of the table
            column_name: Name of the column to analyze

        Returns:
            ColumnMetadata with suggested values
        """
        return suggest_column_metadata(document, table_index, column_name, self.model)

    def infer_relationships(
        self,
        tables: list[TableMetadata],
    ) -> list[TableRelationship]:
        """Infer relationships between tables using LLM.

        Analyzes column names, types, and semantic types to identify
        potential join relationships between tables.

        Args:
            tables: List of TableMetadata to analyze

        Returns:
            List of inferred relationships
        """
        return infer_relationships(tables, self.model)

    def find_potential_joins(
        self,
        tables: list[TableMetadata],
    ) -> dict[str, Any]:
        """Find potential join opportunities without using LLM.

        Uses heuristics to identify:
        - Columns with the same name across tables
        - Columns with matching semantic types

        Args:
            tables: List of TableMetadata to analyze

        Returns:
            Dictionary with common_columns and semantic_matches
        """
        return {
            "common_columns": find_common_columns(tables),
            "semantic_matches": find_semantic_matches(tables),
        }

    def validate_metadata(self, metadata: TableMetadata) -> ValidationResult:
        """Validate metadata for completeness and correctness.

        Checks for:
        - Required fields are present
        - Valid data types and domains
        - Column-level completeness
        - Semantic type suggestions

        Args:
            metadata: TableMetadata to validate

        Returns:
            ValidationResult with issues and completeness score
        """
        return validate_metadata(metadata)

    def check_coverage(
        self,
        metadata: TableMetadata,
        actual_columns: list[str],
    ) -> ValidationResult:
        """Check if all actual columns have metadata.

        Useful after loading real data to verify metadata coverage.

        Args:
            metadata: TableMetadata to check
            actual_columns: Column names from actual data

        Returns:
            ValidationResult with coverage information
        """
        return check_column_coverage(metadata, actual_columns)

    def save_to_catalog(self, metadata: TableMetadata) -> str:
        """Save metadata to the catalog.

        Persists the metadata to the metadata-catalog bucket in MinIO.
        Validates before saving.

        Args:
            metadata: TableMetadata to save

        Returns:
            Path where metadata was stored

        Raises:
            ValueError: If metadata fails validation
            CatalogError: If save fails
        """
        validation = self.validate_metadata(metadata)
        if not validation.is_valid:
            errors = [e.message for e in validation.errors]
            raise ValueError(f"Metadata validation failed: {'; '.join(errors)}")

        return self.catalog.save(metadata)

    def load_from_catalog(
        self,
        agency: str,
        asset: str,
        table_id: str,
    ) -> TableMetadata:
        """Load metadata from the catalog.

        Args:
            agency: Agency name
            asset: Asset name
            table_id: Table identifier

        Returns:
            TableMetadata from catalog
        """
        return self.catalog.load(agency, asset, table_id)

    def catalog_exists(
        self,
        agency: str,
        asset: str,
        table_id: str,
    ) -> bool:
        """Check if metadata exists in the catalog.

        Args:
            agency: Agency name
            asset: Asset name
            table_id: Table identifier

        Returns:
            True if metadata exists
        """
        return self.catalog.exists(agency, asset, table_id)

    def list_cataloged_tables(
        self,
        agency: str,
        asset: str,
    ) -> list[str]:
        """List table IDs with metadata in the catalog.

        Args:
            agency: Agency name
            asset: Asset name

        Returns:
            List of table IDs
        """
        return self.catalog.list_tables(agency, asset)

    def get_all_metadata_for_asset(
        self,
        agency: str,
        asset: str,
    ) -> list[TableMetadata]:
        """Load all table metadata for an asset.

        Args:
            agency: Agency name
            asset: Asset name

        Returns:
            List of TableMetadata objects
        """
        return self.catalog.load_all_for_asset(agency, asset)
