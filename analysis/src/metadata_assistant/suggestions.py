"""LLM-powered metadata suggestion functions.

Uses Claude's tool use capability to generate structured metadata
suggestions for tables and columns.
"""

import os
from typing import Any

from metadata_assistant.models import TableMetadata, ColumnMetadata
from metadata_assistant.prompts import (
    TABLE_METADATA_SYSTEM_PROMPT,
    TABLE_METADATA_TOOL,
    COLUMN_METADATA_SYSTEM_PROMPT,
    COLUMN_METADATA_TOOL,
)


# Default model
DEFAULT_MODEL = "claude-3-5-haiku-20241022"


class SuggestionError(Exception):
    """Error during suggestion generation."""

    pass


def _get_anthropic_client():
    """Get Anthropic client, lazily imported."""
    try:
        import anthropic

        return anthropic.Anthropic()
    except ImportError:
        raise SuggestionError(
            "anthropic package not installed. Install with: pip install anthropic"
        )
    except Exception as e:
        raise SuggestionError(f"Failed to initialize Anthropic client: {e}")


def suggest_table_metadata(
    document: dict[str, Any],
    table_index: int,
    model: str = DEFAULT_MODEL,
) -> TableMetadata:
    """Generate metadata suggestions for a table using LLM.

    Args:
        document: Parsed document containing the table
        table_index: Index of the table in the document
        model: Claude model to use (default: claude-3-5-haiku)

    Returns:
        TableMetadata with LLM-suggested values

    Raises:
        SuggestionError: If suggestion generation fails
    """
    content = document.get("content", {})
    tables = content.get("tables", [])

    if table_index < 0 or table_index >= len(tables):
        raise SuggestionError(
            f"Table index {table_index} out of range. Document has {len(tables)} tables."
        )

    table = tables[table_index]
    metadata = document.get("metadata", {})
    source = document.get("_source", {})

    # Prepare context for LLM
    headers = table.get("headers", [])
    rows = table.get("rows", [])
    sample_rows = rows[:5] if rows else []

    context = f"""Document: {metadata.get('title', 'Unknown')}
Publisher: {metadata.get('publisher', 'Unknown')}
Agency: {source.get('agency', 'Unknown')}
Asset: {source.get('asset', 'Unknown')}

Table: {table.get('title', f'Table {table_index}')}
Columns: {', '.join(str(h) for h in headers)}
Row count: {len(rows)}

Sample data:
"""
    for row in sample_rows:
        context += f"  {row}\n"

    # Call LLM
    client = _get_anthropic_client()
    response = client.messages.create(
        model=model,
        max_tokens=2048,
        system=TABLE_METADATA_SYSTEM_PROMPT,
        tools=[TABLE_METADATA_TOOL],
        tool_choice={"type": "tool", "name": "extract_table_metadata"},
        messages=[
            {
                "role": "user",
                "content": f"Analyze this table and suggest metadata:\n\n{context}",
            }
        ],
    )

    # Parse response
    for block in response.content:
        if block.type == "tool_use" and block.name == "extract_table_metadata":
            return _parse_table_suggestion(
                block.input,
                document,
                table_index,
                headers,
                rows,
            )

    raise SuggestionError("LLM did not return expected tool use response")


def _parse_table_suggestion(
    suggestion: dict[str, Any],
    document: dict[str, Any],
    table_index: int,
    headers: list[str],
    rows: list[list[Any]],
) -> TableMetadata:
    """Parse LLM suggestion into TableMetadata."""
    source = document.get("_source", {})

    # Parse columns
    columns = []
    for col_data in suggestion.get("columns", []):
        # Get sample values for this column
        col_name = col_data.get("column_name", "")
        sample_values = []
        if col_name in headers:
            col_idx = headers.index(col_name)
            for row in rows[:5]:
                if col_idx < len(row) and row[col_idx] is not None:
                    sample_values.append(str(row[col_idx]))

        columns.append(
            ColumnMetadata(
                column_name=col_name,
                display_name=col_data.get("display_name", col_name),
                description=col_data.get("description", ""),
                data_type=col_data.get("data_type", "text"),
                semantic_type=col_data.get("semantic_type"),
                unit=col_data.get("unit"),
                sample_values=sample_values,
            )
        )

    return TableMetadata(
        table_id=f"{source.get('asset', 'unknown')}-table-{table_index}",
        asset=source.get("asset", ""),
        agency=source.get("agency", ""),
        display_name=suggestion.get("display_name", ""),
        description=suggestion.get("description", ""),
        data_domain=suggestion.get("data_domain", "other"),
        columns=columns,
    )


def suggest_column_metadata(
    document: dict[str, Any],
    table_index: int,
    column_name: str,
    model: str = DEFAULT_MODEL,
) -> ColumnMetadata:
    """Generate metadata suggestions for a specific column.

    Args:
        document: Parsed document containing the table
        table_index: Index of the table in the document
        column_name: Name of the column to analyze
        model: Claude model to use

    Returns:
        ColumnMetadata with LLM-suggested values

    Raises:
        SuggestionError: If suggestion generation fails
    """
    content = document.get("content", {})
    tables = content.get("tables", [])

    if table_index < 0 or table_index >= len(tables):
        raise SuggestionError(f"Table index {table_index} out of range.")

    table = tables[table_index]
    headers = table.get("headers", [])
    rows = table.get("rows", [])

    if column_name not in headers:
        raise SuggestionError(f"Column '{column_name}' not found in table.")

    col_idx = headers.index(column_name)
    sample_values = []
    for row in rows[:10]:
        if col_idx < len(row) and row[col_idx] is not None:
            sample_values.append(str(row[col_idx]))

    # Prepare context
    metadata = document.get("metadata", {})
    context = f"""Document: {metadata.get('title', 'Unknown')}
Table: {table.get('title', f'Table {table_index}')}
All columns: {', '.join(str(h) for h in headers)}

Column to analyze: {column_name}
Sample values: {sample_values}
"""

    # Call LLM
    client = _get_anthropic_client()
    response = client.messages.create(
        model=model,
        max_tokens=1024,
        system=COLUMN_METADATA_SYSTEM_PROMPT,
        tools=[COLUMN_METADATA_TOOL],
        tool_choice={"type": "tool", "name": "extract_column_metadata"},
        messages=[
            {
                "role": "user",
                "content": f"Analyze this column and suggest metadata:\n\n{context}",
            }
        ],
    )

    # Parse response
    for block in response.content:
        if block.type == "tool_use" and block.name == "extract_column_metadata":
            return ColumnMetadata(
                column_name=column_name,
                display_name=block.input.get("display_name", column_name),
                description=block.input.get("description", ""),
                data_type=block.input.get("data_type", "text"),
                semantic_type=block.input.get("semantic_type"),
                unit=block.input.get("unit"),
                sample_values=sample_values[:5],
            )

    raise SuggestionError("LLM did not return expected tool use response")
