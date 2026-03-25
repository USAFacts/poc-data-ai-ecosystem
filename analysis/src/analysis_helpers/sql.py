"""DuckDB SQL interface for analyzing data.

Provides an in-process SQL engine for querying pandas DataFrames
without requiring a separate database server.
"""

from typing import Any

import duckdb
import pandas as pd


class SQLContext:
    """SQL context for querying DataFrames with DuckDB.

    Provides a simple interface to register DataFrames as tables
    and run SQL queries against them.

    Example:
        >>> ctx = SQLContext()
        >>> ctx.register("forms", df)
        >>> result = ctx.query("SELECT * FROM forms WHERE fiscal_year = 2024")
    """

    def __init__(self) -> None:
        """Initialize the SQL context."""
        self._connection = duckdb.connect(":memory:")
        self._tables: dict[str, pd.DataFrame] = {}

    def register(self, name: str, df: pd.DataFrame) -> None:
        """Register a DataFrame as a table.

        Args:
            name: Table name for SQL queries
            df: pandas DataFrame to register
        """
        self._tables[name] = df
        self._connection.register(name, df)

    def unregister(self, name: str) -> None:
        """Unregister a table.

        Args:
            name: Table name to unregister
        """
        if name in self._tables:
            del self._tables[name]
            self._connection.unregister(name)

    def query(self, sql: str) -> pd.DataFrame:
        """Execute a SQL query and return results as DataFrame.

        Args:
            sql: SQL query string

        Returns:
            Query results as pandas DataFrame
        """
        return self._connection.execute(sql).fetchdf()

    def tables(self) -> list[str]:
        """List registered table names.

        Returns:
            List of table names
        """
        return list(self._tables.keys())

    def describe(self, table_name: str) -> pd.DataFrame:
        """Get column info for a table.

        Args:
            table_name: Name of table to describe

        Returns:
            DataFrame with column names and types
        """
        return self.query(f"DESCRIBE {table_name}")

    def close(self) -> None:
        """Close the connection."""
        self._connection.close()

    def __enter__(self) -> "SQLContext":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


def query_sql(tables: dict[str, pd.DataFrame], sql: str) -> pd.DataFrame:
    """Execute a SQL query against multiple DataFrames.

    This is a convenience function for one-off queries. For multiple
    queries, use SQLContext directly to avoid re-registering tables.

    Args:
        tables: Dictionary mapping table names to DataFrames
        sql: SQL query string

    Returns:
        Query results as pandas DataFrame

    Example:
        >>> result = query_sql(
        ...     {"forms": df1, "approvals": df2},
        ...     "SELECT f.*, a.rate FROM forms f JOIN approvals a ON f.id = a.form_id"
        ... )
    """
    with SQLContext() as ctx:
        for name, df in tables.items():
            ctx.register(name, df)
        return ctx.query(sql)
