"""Tests for the MinioExplorer and analysis helpers."""

import pytest
import pandas as pd

from analysis_helpers.explorer import MinioExplorer, AssetInfo, ZONES
from analysis_helpers.sql import SQLContext, query_sql


class TestAssetInfo:
    """Tests for AssetInfo dataclass."""

    def test_asset_info_path(self):
        """Test path property."""
        asset = AssetInfo(
            agency="uscis",
            asset="quarterly-forms",
            zone="parsed-zone",
            versions=["2024-01-15/120000"],
            latest_version="2024-01-15/120000",
        )
        assert asset.path == "uscis/quarterly-forms"

    def test_asset_info_no_versions(self):
        """Test with no versions."""
        asset = AssetInfo(
            agency="test",
            asset="test-asset",
            zone="landing-zone",
            versions=[],
            latest_version=None,
        )
        assert asset.latest_version is None


class TestSQLContext:
    """Tests for SQLContext."""

    def test_register_and_query(self):
        """Test registering a DataFrame and querying."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

        ctx = SQLContext()
        ctx.register("test", df)

        assert "test" in ctx.tables()

        result = ctx.query("SELECT * FROM test WHERE a > 1")
        assert len(result) == 2
        assert list(result["a"]) == [2, 3]

    def test_unregister(self):
        """Test unregistering a table."""
        df = pd.DataFrame({"x": [1]})

        ctx = SQLContext()
        ctx.register("t", df)
        assert "t" in ctx.tables()

        ctx.unregister("t")
        assert "t" not in ctx.tables()

    def test_describe(self):
        """Test describe table."""
        df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})

        ctx = SQLContext()
        ctx.register("data", df)

        desc = ctx.describe("data")
        assert "column_name" in desc.columns
        assert len(desc) == 2

    def test_context_manager(self):
        """Test context manager usage."""
        df = pd.DataFrame({"val": [10, 20]})

        with SQLContext() as ctx:
            ctx.register("nums", df)
            result = ctx.query("SELECT SUM(val) as total FROM nums")
            assert result["total"].iloc[0] == 30


class TestQuerySQL:
    """Tests for query_sql convenience function."""

    def test_single_table_query(self):
        """Test querying a single table."""
        df = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})

        result = query_sql({"data": df}, "SELECT AVG(value) as avg_val FROM data")
        assert result["avg_val"].iloc[0] == 20.0

    def test_multi_table_join(self):
        """Test joining multiple tables."""
        orders = pd.DataFrame({"order_id": [1, 2], "customer_id": [10, 20]})
        customers = pd.DataFrame({"id": [10, 20], "name": ["Alice", "Bob"]})

        result = query_sql(
            {"orders": orders, "customers": customers},
            """
            SELECT o.order_id, c.name
            FROM orders o
            JOIN customers c ON o.customer_id = c.id
            ORDER BY o.order_id
            """,
        )

        assert len(result) == 2
        assert list(result["name"]) == ["Alice", "Bob"]


class TestZones:
    """Tests for zone constants."""

    def test_zones_defined(self):
        """Test that expected zones are defined."""
        assert "landing-zone" in ZONES
        assert "parsed-zone" in ZONES
        assert "enrichment-zone" in ZONES
        assert "ready-zone" in ZONES
