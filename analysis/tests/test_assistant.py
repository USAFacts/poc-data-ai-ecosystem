"""Tests for the MetadataAssistant and related components."""

import pytest
from datetime import datetime, timezone

from metadata_assistant.models import (
    TableMetadata,
    ColumnMetadata,
    TableRelationship,
    ValidationResult,
    ValidationIssue,
)
from metadata_assistant.validation import (
    validate_metadata,
    check_column_coverage,
    _calculate_completeness,
)


class TestColumnMetadata:
    """Tests for ColumnMetadata dataclass."""

    def test_to_dict(self):
        """Test serialization to dict."""
        col = ColumnMetadata(
            column_name="fiscal_year",
            display_name="Fiscal Year",
            description="The fiscal year of the data",
            data_type="numeric",
            semantic_type="fiscal_year",
            unit=None,
            sample_values=["2023", "2024"],
        )

        d = col.to_dict()
        assert d["columnName"] == "fiscal_year"
        assert d["displayName"] == "Fiscal Year"
        assert d["dataType"] == "numeric"
        assert d["semanticType"] == "fiscal_year"
        assert "unit" not in d  # None values excluded
        assert d["sampleValues"] == ["2023", "2024"]

    def test_from_dict(self):
        """Test deserialization from dict."""
        d = {
            "columnName": "rate",
            "displayName": "Rate",
            "description": "A rate value",
            "dataType": "percentage",
            "unit": "percent",
        }

        col = ColumnMetadata.from_dict(d)
        assert col.column_name == "rate"
        assert col.unit == "percent"
        assert col.semantic_type is None


class TestTableMetadata:
    """Tests for TableMetadata dataclass."""

    def test_to_dict(self):
        """Test serialization."""
        metadata = TableMetadata(
            table_id="test-table-0",
            asset="test-asset",
            agency="test-agency",
            display_name="Test Table",
            description="A test table",
            data_domain="other",
            columns=[
                ColumnMetadata(
                    column_name="col1",
                    display_name="Column 1",
                    description="First column",
                    data_type="text",
                )
            ],
        )

        d = metadata.to_dict()
        assert d["tableId"] == "test-table-0"
        assert d["dataDomain"] == "other"
        assert len(d["columns"]) == 1
        assert d["columns"][0]["columnName"] == "col1"

    def test_from_dict(self):
        """Test deserialization."""
        d = {
            "tableId": "t1",
            "asset": "a1",
            "agency": "ag1",
            "displayName": "Table One",
            "description": "Description",
            "dataDomain": "immigration",
            "columns": [],
            "relationships": [],
            "curatedAt": "2024-01-15T10:00:00+00:00",
            "curatedBy": "analyst",
        }

        metadata = TableMetadata.from_dict(d)
        assert metadata.table_id == "t1"
        assert metadata.data_domain == "immigration"
        assert metadata.curated_by == "analyst"


class TestTableRelationship:
    """Tests for TableRelationship dataclass."""

    def test_to_dict(self):
        """Test serialization."""
        rel = TableRelationship(
            related_table_id="table-2",
            relationship_type="one-to-many",
            source_column="id",
            target_column="parent_id",
            description="Parent-child relationship",
        )

        d = rel.to_dict()
        assert d["relatedTableId"] == "table-2"
        assert d["relationshipType"] == "one-to-many"
        assert d["description"] == "Parent-child relationship"


class TestValidation:
    """Tests for metadata validation."""

    def test_valid_metadata(self):
        """Test validation of complete metadata."""
        metadata = TableMetadata(
            table_id="test-0",
            asset="test",
            agency="agency",
            display_name="Test Table",
            description="A complete description of the test table with details.",
            data_domain="immigration",
            columns=[
                ColumnMetadata(
                    column_name="col1",
                    display_name="Column One",
                    description="First column description",
                    data_type="text",
                )
            ],
        )

        result = validate_metadata(metadata)
        assert result.is_valid
        assert len(result.errors) == 0

    def test_missing_required_fields(self):
        """Test validation catches missing required fields."""
        metadata = TableMetadata(
            table_id="",  # Empty
            asset="test",
            agency="agency",
            display_name="",  # Empty
            description="",  # Empty
            data_domain="",  # Empty
        )

        result = validate_metadata(metadata)
        assert not result.is_valid
        assert len(result.errors) >= 4

    def test_short_description_warning(self):
        """Test warning for short description."""
        metadata = TableMetadata(
            table_id="t1",
            asset="test",
            agency="agency",
            display_name="Table",
            description="Short",  # Too short
            data_domain="other",
        )

        result = validate_metadata(metadata)
        assert any(
            issue.severity == "warning" and "short" in issue.message.lower()
            for issue in result.issues
        )

    def test_unknown_data_domain_warning(self):
        """Test warning for unknown data domain."""
        metadata = TableMetadata(
            table_id="t1",
            asset="test",
            agency="agency",
            display_name="Table",
            description="A valid description here.",
            data_domain="unknown-domain",
        )

        result = validate_metadata(metadata)
        assert any(
            issue.severity == "warning" and "domain" in issue.message.lower()
            for issue in result.issues
        )


class TestColumnCoverage:
    """Tests for column coverage checking."""

    def test_full_coverage(self):
        """Test when all columns have metadata."""
        metadata = TableMetadata(
            table_id="t1",
            asset="test",
            agency="agency",
            display_name="Table",
            description="Description",
            data_domain="other",
            columns=[
                ColumnMetadata("a", "A", "Column A", "text"),
                ColumnMetadata("b", "B", "Column B", "numeric"),
            ],
        )

        result = check_column_coverage(metadata, ["a", "b"])
        assert result.is_valid
        assert result.completeness_score == 1.0

    def test_missing_columns(self):
        """Test when some columns lack metadata."""
        metadata = TableMetadata(
            table_id="t1",
            asset="test",
            agency="agency",
            display_name="Table",
            description="Description",
            data_domain="other",
            columns=[
                ColumnMetadata("a", "A", "Column A", "text"),
            ],
        )

        result = check_column_coverage(metadata, ["a", "b", "c"])
        assert not result.is_valid
        assert result.completeness_score < 1.0
        assert any("b" in issue.message for issue in result.issues)
        assert any("c" in issue.message for issue in result.issues)


class TestCompleteness:
    """Tests for completeness calculation."""

    def test_empty_metadata(self):
        """Test completeness of empty metadata."""
        metadata = TableMetadata(
            table_id="",
            asset="",
            agency="",
            display_name="",
            description="",
            data_domain="",
        )

        score = _calculate_completeness(metadata)
        assert score == 0.0

    def test_partial_completeness(self):
        """Test partial completeness."""
        metadata = TableMetadata(
            table_id="t1",
            asset="",  # Empty
            agency="agency",
            display_name="",  # Empty
            description="desc",
            data_domain="other",
        )

        score = _calculate_completeness(metadata)
        assert 0.0 < score < 1.0
