"""Data models for metadata curation.

These dataclasses define the structure for table and column metadata
that can be curated and persisted to the catalog.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class ColumnMetadata:
    """Metadata for a table column.

    Attributes:
        column_name: Original column name from the data
        display_name: Human-readable name for display
        description: Detailed description of what this column contains
        data_type: Data type category (category, numeric, percentage, currency, date, text)
        semantic_type: Optional semantic type (fiscal_year, approval_rate, country_code, etc.)
        unit: Optional unit of measurement
        sample_values: Example values from the column
    """

    column_name: str
    display_name: str
    description: str
    data_type: str
    semantic_type: str | None = None
    unit: str | None = None
    sample_values: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        result: dict[str, Any] = {
            "columnName": self.column_name,
            "displayName": self.display_name,
            "description": self.description,
            "dataType": self.data_type,
        }
        if self.semantic_type:
            result["semanticType"] = self.semantic_type
        if self.unit:
            result["unit"] = self.unit
        if self.sample_values:
            result["sampleValues"] = self.sample_values
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ColumnMetadata":
        """Create from dictionary."""
        return cls(
            column_name=data.get("columnName", ""),
            display_name=data.get("displayName", ""),
            description=data.get("description", ""),
            data_type=data.get("dataType", "text"),
            semantic_type=data.get("semanticType"),
            unit=data.get("unit"),
            sample_values=data.get("sampleValues", []),
        )


@dataclass
class TableRelationship:
    """Relationship between tables.

    Defines how tables can be joined or related.

    Attributes:
        related_table_id: ID of the related table
        relationship_type: Type of relationship (one-to-many, many-to-one, many-to-many)
        source_column: Column in this table
        target_column: Column in the related table
        description: Description of the relationship
    """

    related_table_id: str
    relationship_type: str
    source_column: str
    target_column: str
    description: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        result: dict[str, Any] = {
            "relatedTableId": self.related_table_id,
            "relationshipType": self.relationship_type,
            "sourceColumn": self.source_column,
            "targetColumn": self.target_column,
        }
        if self.description:
            result["description"] = self.description
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TableRelationship":
        """Create from dictionary."""
        return cls(
            related_table_id=data.get("relatedTableId", ""),
            relationship_type=data.get("relationshipType", ""),
            source_column=data.get("sourceColumn", ""),
            target_column=data.get("targetColumn", ""),
            description=data.get("description"),
        )


@dataclass
class TableMetadata:
    """Complete metadata for a table.

    Contains all curated information about a table including
    its columns, relationships, and provenance.

    Attributes:
        table_id: Unique identifier for this table
        asset: Source asset name
        agency: Source agency name
        display_name: Human-readable table name
        description: Detailed description of the table
        data_domain: Domain category (immigration, demographics, economics, etc.)
        columns: List of column metadata
        relationships: List of relationships to other tables
        curated_at: When this metadata was last curated
        curated_by: Who curated this metadata
    """

    table_id: str
    asset: str
    agency: str
    display_name: str
    description: str
    data_domain: str
    columns: list[ColumnMetadata] = field(default_factory=list)
    relationships: list[TableRelationship] = field(default_factory=list)
    curated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    curated_by: str = "analyst"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "tableId": self.table_id,
            "asset": self.asset,
            "agency": self.agency,
            "displayName": self.display_name,
            "description": self.description,
            "dataDomain": self.data_domain,
            "columns": [c.to_dict() for c in self.columns],
            "relationships": [r.to_dict() for r in self.relationships],
            "curatedAt": self.curated_at.isoformat(),
            "curatedBy": self.curated_by,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TableMetadata":
        """Create from dictionary."""
        curated_at = data.get("curatedAt")
        if isinstance(curated_at, str):
            curated_at = datetime.fromisoformat(curated_at.replace("Z", "+00:00"))
        else:
            curated_at = datetime.now(timezone.utc)

        return cls(
            table_id=data.get("tableId", ""),
            asset=data.get("asset", ""),
            agency=data.get("agency", ""),
            display_name=data.get("displayName", ""),
            description=data.get("description", ""),
            data_domain=data.get("dataDomain", ""),
            columns=[ColumnMetadata.from_dict(c) for c in data.get("columns", [])],
            relationships=[TableRelationship.from_dict(r) for r in data.get("relationships", [])],
            curated_at=curated_at,
            curated_by=data.get("curatedBy", "analyst"),
        )


@dataclass
class ValidationIssue:
    """An issue found during validation.

    Attributes:
        field: Field with the issue
        severity: Issue severity (error, warning, info)
        message: Description of the issue
    """

    field: str
    severity: str
    message: str


@dataclass
class ValidationResult:
    """Result of metadata validation.

    Attributes:
        is_valid: Whether the metadata passes validation
        issues: List of validation issues
        completeness_score: Percentage of metadata fields that are complete
    """

    is_valid: bool
    issues: list[ValidationIssue] = field(default_factory=list)
    completeness_score: float = 0.0

    @property
    def errors(self) -> list[ValidationIssue]:
        """Get only error-level issues."""
        return [i for i in self.issues if i.severity == "error"]

    @property
    def warnings(self) -> list[ValidationIssue]:
        """Get only warning-level issues."""
        return [i for i in self.issues if i.severity == "warning"]
