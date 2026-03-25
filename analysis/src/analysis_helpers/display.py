"""Jupyter display helpers for data exploration.

Provides formatted output for notebooks including table previews,
asset trees, and summary statistics.
"""

from typing import Any

import pandas as pd


def display_table_preview(
    df: pd.DataFrame,
    title: str | None = None,
    max_rows: int = 10,
    max_cols: int = 10,
) -> None:
    """Display a formatted table preview in Jupyter.

    Args:
        df: DataFrame to display
        title: Optional title to show above the table
        max_rows: Maximum rows to display (default: 10)
        max_cols: Maximum columns to display (default: 10)
    """
    try:
        from IPython.display import display, HTML
    except ImportError:
        # Fall back to plain print if not in Jupyter
        if title:
            print(f"\n{title}")
        print(df.head(max_rows).to_string())
        return

    # Build summary
    rows, cols = df.shape
    summary_parts = [f"{rows:,} rows", f"{cols} columns"]

    # Add memory usage
    memory = df.memory_usage(deep=True).sum()
    if memory > 1024 * 1024:
        summary_parts.append(f"{memory / (1024 * 1024):.1f} MB")
    elif memory > 1024:
        summary_parts.append(f"{memory / 1024:.1f} KB")

    summary = " | ".join(summary_parts)

    # Truncate columns if needed
    display_df = df.iloc[:max_rows]
    if cols > max_cols:
        display_df = display_df.iloc[:, :max_cols]
        summary += f" (showing first {max_cols} columns)"

    # Build HTML
    html_parts = []

    if title:
        html_parts.append(f"<h4 style='margin-bottom: 5px;'>{title}</h4>")

    html_parts.append(f"<p style='color: #666; margin: 5px 0;'>{summary}</p>")
    html_parts.append(display_df.to_html(max_rows=max_rows))

    display(HTML("".join(html_parts)))


def display_asset_tree(
    assets: list[Any],
    title: str = "Available Assets",
) -> None:
    """Display assets as a tree structure.

    Args:
        assets: List of AssetInfo objects or dicts with agency/asset keys
        title: Title for the tree (default: "Available Assets")
    """
    try:
        from IPython.display import display, HTML
    except ImportError:
        # Fall back to plain print
        print(f"\n{title}")
        for asset in assets:
            if hasattr(asset, "agency"):
                print(f"  {asset.agency}/{asset.asset}")
            else:
                print(f"  {asset.get('agency', '?')}/{asset.get('asset', '?')}")
        return

    # Group by agency
    by_agency: dict[str, list] = {}
    for asset in assets:
        if hasattr(asset, "agency"):
            agency = asset.agency
            name = asset.asset
            versions = getattr(asset, "versions", [])
        else:
            agency = asset.get("agency", "unknown")
            name = asset.get("asset", "unknown")
            versions = asset.get("versions", [])

        if agency not in by_agency:
            by_agency[agency] = []
        by_agency[agency].append({"name": name, "versions": versions})

    # Build HTML tree
    html = [f"<div style='font-family: monospace;'>"]
    html.append(f"<strong>{title}</strong>")
    html.append("<ul style='list-style-type: none; padding-left: 0;'>")

    for agency in sorted(by_agency.keys()):
        html.append(f"<li style='margin: 5px 0;'>")
        html.append(f"<span style='color: #0066cc;'>{agency}/</span>")
        html.append("<ul style='list-style-type: none; padding-left: 20px;'>")

        for asset in sorted(by_agency[agency], key=lambda a: a["name"]):
            version_count = len(asset["versions"])
            version_info = f"({version_count} version{'s' if version_count != 1 else ''})"
            html.append(
                f"<li><span style='color: #333;'>{asset['name']}</span> "
                f"<span style='color: #999; font-size: 0.9em;'>{version_info}</span></li>"
            )

        html.append("</ul></li>")

    html.append("</ul></div>")

    display(HTML("".join(html)))


def display_document_summary(document: dict[str, Any]) -> None:
    """Display a summary of a parsed document.

    Args:
        document: Parsed document dictionary
    """
    try:
        from IPython.display import display, HTML
    except ImportError:
        # Fall back to plain print
        metadata = document.get("metadata", {})
        print(f"\nTitle: {metadata.get('title', 'Untitled')}")
        print(f"Publisher: {metadata.get('publisher', 'Unknown')}")
        return

    metadata = document.get("metadata", {})
    content = document.get("content", {})
    source = document.get("_source", {})

    html = ["<div style='border: 1px solid #ddd; padding: 15px; border-radius: 5px;'>"]

    # Title
    title = metadata.get("title", "Untitled Document")
    html.append(f"<h3 style='margin-top: 0;'>{title}</h3>")

    # Source info
    if source:
        path = f"{source.get('agency', '?')}/{source.get('asset', '?')}"
        zone = source.get("zone", "?")
        version = source.get("version", "?")
        html.append(
            f"<p style='color: #666;'>Source: {path} | Zone: {zone} | Version: {version}</p>"
        )

    # Content summary
    sections = content.get("sections", [])
    tables = content.get("tables", [])

    html.append("<div style='display: flex; gap: 20px; margin-top: 10px;'>")
    html.append(
        f"<div style='background: #f5f5f5; padding: 10px; border-radius: 3px;'>"
        f"<strong>{len(sections)}</strong> sections</div>"
    )
    html.append(
        f"<div style='background: #f5f5f5; padding: 10px; border-radius: 3px;'>"
        f"<strong>{len(tables)}</strong> tables</div>"
    )
    html.append("</div>")

    # Table summary if present
    if tables:
        html.append("<h4 style='margin-top: 15px;'>Tables:</h4>")
        html.append("<ul style='margin: 5px 0;'>")
        for i, table in enumerate(tables[:5]):
            table_title = table.get("title", f"Table {i}")
            rows = len(table.get("rows", []))
            cols = len(table.get("headers", []))
            html.append(f"<li>{table_title} ({rows} rows, {cols} columns)</li>")
        if len(tables) > 5:
            html.append(f"<li>... and {len(tables) - 5} more tables</li>")
        html.append("</ul>")

    html.append("</div>")

    display(HTML("".join(html)))
