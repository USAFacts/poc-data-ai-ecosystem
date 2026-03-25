"""Metadata validation and completeness checking.

Provides functions to validate curated metadata and calculate
completeness scores.
"""

from metadata_assistant.models import (
    TableMetadata,
    ColumnMetadata,
    ValidationResult,
    ValidationIssue,
)


def validate_metadata(metadata: TableMetadata) -> ValidationResult:
    """Validate table metadata for completeness and correctness.

    Checks for:
    - Required fields are present and non-empty
    - Data types are valid
    - Semantic types follow conventions
    - Column metadata is complete

    Args:
        metadata: TableMetadata to validate

    Returns:
        ValidationResult with issues and completeness score
    """
    issues: list[ValidationIssue] = []

    # Required fields
    if not metadata.table_id:
        issues.append(
            ValidationIssue(field="table_id", severity="error", message="Table ID is required")
        )

    if not metadata.display_name:
        issues.append(
            ValidationIssue(
                field="display_name", severity="error", message="Display name is required"
            )
        )

    if not metadata.description:
        issues.append(
            ValidationIssue(
                field="description", severity="error", message="Description is required"
            )
        )
    elif len(metadata.description) < 20:
        issues.append(
            ValidationIssue(
                field="description",
                severity="warning",
                message="Description is very short. Consider adding more detail.",
            )
        )

    if not metadata.data_domain:
        issues.append(
            ValidationIssue(
                field="data_domain", severity="error", message="Data domain is required"
            )
        )

    # Valid data domains
    valid_domains = {
        "immigration",
        "demographics",
        "economics",
        "employment",
        "education",
        "healthcare",
        "housing",
        "environment",
        "transportation",
        "public-safety",
        "other",
    }
    if metadata.data_domain and metadata.data_domain not in valid_domains:
        issues.append(
            ValidationIssue(
                field="data_domain",
                severity="warning",
                message=f"Unknown data domain: {metadata.data_domain}. "
                f"Consider using one of: {', '.join(sorted(valid_domains))}",
            )
        )

    # Column validation
    if not metadata.columns:
        issues.append(
            ValidationIssue(
                field="columns",
                severity="warning",
                message="No column metadata defined",
            )
        )
    else:
        for i, col in enumerate(metadata.columns):
            col_issues = _validate_column(col, i)
            issues.extend(col_issues)

    # Calculate completeness score
    completeness = _calculate_completeness(metadata)

    # Determine if valid (no errors)
    is_valid = not any(issue.severity == "error" for issue in issues)

    return ValidationResult(
        is_valid=is_valid,
        issues=issues,
        completeness_score=completeness,
    )


def _validate_column(column: ColumnMetadata, index: int) -> list[ValidationIssue]:
    """Validate a single column's metadata."""
    issues: list[ValidationIssue] = []
    prefix = f"columns[{index}]"

    if not column.column_name:
        issues.append(
            ValidationIssue(
                field=f"{prefix}.column_name",
                severity="error",
                message="Column name is required",
            )
        )

    if not column.display_name:
        issues.append(
            ValidationIssue(
                field=f"{prefix}.display_name",
                severity="error",
                message=f"Display name required for column '{column.column_name}'",
            )
        )

    if not column.description:
        issues.append(
            ValidationIssue(
                field=f"{prefix}.description",
                severity="warning",
                message=f"No description for column '{column.column_name}'",
            )
        )

    # Valid data types
    valid_types = {"category", "numeric", "percentage", "currency", "date", "text", "identifier"}
    if column.data_type and column.data_type not in valid_types:
        issues.append(
            ValidationIssue(
                field=f"{prefix}.data_type",
                severity="warning",
                message=f"Unknown data type '{column.data_type}' for column '{column.column_name}'",
            )
        )

    # Suggest semantic type for common patterns
    if not column.semantic_type:
        suggestion = _suggest_semantic_type(column.column_name, column.data_type)
        if suggestion:
            issues.append(
                ValidationIssue(
                    field=f"{prefix}.semantic_type",
                    severity="info",
                    message=f"Consider adding semantic_type='{suggestion}' for column '{column.column_name}'",
                )
            )

    return issues


def _suggest_semantic_type(column_name: str, data_type: str) -> str | None:
    """Suggest a semantic type based on column name patterns."""
    name_lower = column_name.lower()

    patterns = {
        "fiscal_year": ["fiscal_year", "fy", "fiscal year"],
        "calendar_year": ["year", "calendar_year", "yr"],
        "quarter": ["quarter", "qtr", "q1", "q2", "q3", "q4"],
        "month": ["month", "mon"],
        "country_code": ["country", "country_code", "nationality"],
        "state_code": ["state", "state_code"],
        "form_type": ["form", "form_type", "form_number"],
        "approval_rate": ["approval", "approved", "approval_rate"],
        "denial_rate": ["denial", "denied", "denial_rate"],
        "processing_count": ["count", "total", "number"],
    }

    for semantic_type, keywords in patterns.items():
        for keyword in keywords:
            if keyword in name_lower:
                return semantic_type

    return None


def _calculate_completeness(metadata: TableMetadata) -> float:
    """Calculate metadata completeness score (0.0 to 1.0)."""
    total_fields = 0
    filled_fields = 0

    # Table-level fields
    table_fields = [
        metadata.table_id,
        metadata.display_name,
        metadata.description,
        metadata.data_domain,
        metadata.agency,
        metadata.asset,
    ]
    total_fields += len(table_fields)
    filled_fields += sum(1 for f in table_fields if f)

    # Column-level fields
    for col in metadata.columns:
        col_fields = [
            col.column_name,
            col.display_name,
            col.description,
            col.data_type,
            col.semantic_type,
        ]
        total_fields += len(col_fields)
        filled_fields += sum(1 for f in col_fields if f)

    if total_fields == 0:
        return 0.0

    return filled_fields / total_fields


def check_column_coverage(
    metadata: TableMetadata,
    actual_columns: list[str],
) -> ValidationResult:
    """Check if all actual columns have metadata.

    Args:
        metadata: TableMetadata to check
        actual_columns: List of column names from the actual data

    Returns:
        ValidationResult with coverage issues
    """
    issues: list[ValidationIssue] = []

    metadata_columns = {col.column_name for col in metadata.columns}
    actual_set = set(actual_columns)

    # Columns in data but not in metadata
    missing = actual_set - metadata_columns
    for col in sorted(missing):
        issues.append(
            ValidationIssue(
                field="columns",
                severity="warning",
                message=f"Column '{col}' exists in data but has no metadata",
            )
        )

    # Columns in metadata but not in data
    extra = metadata_columns - actual_set
    for col in sorted(extra):
        issues.append(
            ValidationIssue(
                field="columns",
                severity="info",
                message=f"Metadata exists for column '{col}' which is not in current data",
            )
        )

    coverage = len(metadata_columns & actual_set) / len(actual_set) if actual_set else 1.0

    return ValidationResult(
        is_valid=len(missing) == 0,
        issues=issues,
        completeness_score=coverage,
    )
