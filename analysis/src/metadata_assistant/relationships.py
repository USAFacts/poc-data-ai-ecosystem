"""Cross-table relationship inference.

Uses LLM to identify potential relationships between tables
based on column names, data types, and sample values.
"""

from typing import Any

from metadata_assistant.models import TableMetadata, TableRelationship
from metadata_assistant.prompts import RELATIONSHIP_SYSTEM_PROMPT, RELATIONSHIP_TOOL


DEFAULT_MODEL = "claude-3-5-haiku-20241022"


class RelationshipError(Exception):
    """Error during relationship inference."""

    pass


def _get_anthropic_client():
    """Get Anthropic client, lazily imported."""
    try:
        import anthropic

        return anthropic.Anthropic()
    except ImportError:
        raise RelationshipError(
            "anthropic package not installed. Install with: pip install anthropic"
        )
    except Exception as e:
        raise RelationshipError(f"Failed to initialize Anthropic client: {e}")


def infer_relationships(
    tables: list[TableMetadata],
    model: str = DEFAULT_MODEL,
) -> list[TableRelationship]:
    """Infer relationships between tables using LLM.

    Analyzes column names, data types, and semantic types to
    identify potential join relationships between tables.

    Args:
        tables: List of TableMetadata objects to analyze
        model: Claude model to use

    Returns:
        List of inferred TableRelationship objects

    Raises:
        RelationshipError: If inference fails
    """
    if len(tables) < 2:
        return []

    # Prepare table descriptions
    table_descriptions = []
    for table in tables:
        col_info = []
        for col in table.columns:
            semantic = f" ({col.semantic_type})" if col.semantic_type else ""
            col_info.append(f"    - {col.column_name}: {col.data_type}{semantic}")

        table_descriptions.append(
            f"""Table: {table.table_id}
  Display Name: {table.display_name}
  Domain: {table.data_domain}
  Columns:
{chr(10).join(col_info)}"""
        )

    context = "\n\n".join(table_descriptions)

    # Call LLM
    client = _get_anthropic_client()
    response = client.messages.create(
        model=model,
        max_tokens=2048,
        system=RELATIONSHIP_SYSTEM_PROMPT,
        tools=[RELATIONSHIP_TOOL],
        tool_choice={"type": "tool", "name": "infer_relationships"},
        messages=[
            {
                "role": "user",
                "content": f"Analyze these tables and identify relationships:\n\n{context}",
            }
        ],
    )

    # Parse response
    for block in response.content:
        if block.type == "tool_use" and block.name == "infer_relationships":
            return _parse_relationships(block.input, tables)

    raise RelationshipError("LLM did not return expected tool use response")


def _parse_relationships(
    response: dict[str, Any],
    tables: list[TableMetadata],
) -> list[TableRelationship]:
    """Parse LLM response into TableRelationship objects."""
    table_ids = {t.table_id for t in tables}
    relationships = []

    for rel_data in response.get("relationships", []):
        source = rel_data.get("source_table", "")
        target = rel_data.get("target_table", "")

        # Validate that tables exist
        if source not in table_ids or target not in table_ids:
            continue

        relationships.append(
            TableRelationship(
                related_table_id=target,
                relationship_type=rel_data.get("relationship_type", "many-to-many"),
                source_column=rel_data.get("source_column", ""),
                target_column=rel_data.get("target_column", ""),
                description=rel_data.get("description"),
            )
        )

    return relationships


def find_common_columns(
    tables: list[TableMetadata],
) -> dict[str, list[str]]:
    """Find columns that appear in multiple tables.

    This is a simple heuristic-based approach that doesn't require LLM.
    Useful for quick relationship discovery.

    Args:
        tables: List of TableMetadata objects

    Returns:
        Dictionary mapping column names to list of table IDs containing them
    """
    column_to_tables: dict[str, list[str]] = {}

    for table in tables:
        for col in table.columns:
            name = col.column_name.lower()
            if name not in column_to_tables:
                column_to_tables[name] = []
            column_to_tables[name].append(table.table_id)

    # Filter to columns in multiple tables
    return {col: tables for col, tables in column_to_tables.items() if len(tables) > 1}


def find_semantic_matches(
    tables: list[TableMetadata],
) -> list[tuple[str, str, str, str]]:
    """Find columns with matching semantic types across tables.

    Args:
        tables: List of TableMetadata objects

    Returns:
        List of tuples (table1_id, column1, table2_id, column2) for matching columns
    """
    # Build index of semantic types to columns
    semantic_index: dict[str, list[tuple[str, str]]] = {}

    for table in tables:
        for col in table.columns:
            if col.semantic_type:
                key = col.semantic_type
                if key not in semantic_index:
                    semantic_index[key] = []
                semantic_index[key].append((table.table_id, col.column_name))

    # Find matches
    matches = []
    for semantic_type, columns in semantic_index.items():
        if len(columns) > 1:
            # Create pairs
            for i, (table1, col1) in enumerate(columns):
                for table2, col2 in columns[i + 1 :]:
                    if table1 != table2:  # Don't match within same table
                        matches.append((table1, col1, table2, col2))

    return matches
