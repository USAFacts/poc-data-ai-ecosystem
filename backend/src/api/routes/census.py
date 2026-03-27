"""Census Bureau Data API routes.

Exposes Census data tools as REST endpoints and provides a Claude-powered
natural language interface for querying Census statistics.
"""

import json
import logging
import os
import time
from typing import Any

import anthropic
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from src.api.deps import DBSession
from src.services.census import (
    CENSUS_TOOLS,
    execute_census_tool,
    fetch_aggregate_data,
    fetch_dataset_geography,
    list_datasets,
    resolve_geography_fips,
    search_data_tables,
)

logger = logging.getLogger(__name__)

router = APIRouter()


# ── Pydantic models ──────────────────────────────────────────

class GeographySearchRequest(BaseModel):
    geography_name: str = Field(..., description="Name to search (e.g. 'California')")
    summary_level: str | None = Field(None, description="Optional level filter (e.g. 'State')")


class DataTableSearchRequest(BaseModel):
    data_table_id: str | None = Field(None, description="Table ID prefix")
    label_query: str | None = Field(None, description="Fuzzy label search")
    api_endpoint: str | None = Field(None, description="Filter by endpoint")
    limit: int = Field(20, ge=1, le=100)


class FetchDataRequest(BaseModel):
    dataset: str = Field(..., description="Dataset path (e.g. 'acs/acs1')")
    year: int = Field(..., description="Data year")
    variables: list[str] | None = Field(None, max_length=50)
    group: str | None = None
    for_clause: str | None = Field(None, description="Geography 'for' param")
    in_clause: str | None = Field(None, description="Geography 'in' param")
    ucgid: str | None = None
    predicates: dict[str, str] | None = None
    descriptive: bool = False


class DatasetGeographyRequest(BaseModel):
    dataset: str = Field(..., description="Dataset path")
    year: int | None = None


class CensusQueryRequest(BaseModel):
    """Natural language Census data query — Claude handles tool orchestration."""
    query: str = Field(..., description="Natural language question about Census data")


class CensusQueryResponse(BaseModel):
    answer: str
    tool_calls: list[dict[str, Any]] = []
    token_usage: dict[str, int] = {}
    elapsed_ms: int = 0


# ── Direct tool endpoints ────────────────────────────────────

@router.post("/geography/search")
def geography_search(request: GeographySearchRequest, db: DBSession) -> dict[str, Any]:
    """Search for geographies by name and get FIPS codes + Census API params."""
    results = resolve_geography_fips(db, request.geography_name, request.summary_level)
    return {"results": results, "count": len(results)}


@router.post("/data-tables/search")
def data_table_search(request: DataTableSearchRequest, db: DBSession) -> dict[str, Any]:
    """Search Census data table catalog by ID, label, or endpoint."""
    if not any([request.data_table_id, request.label_query, request.api_endpoint]):
        raise HTTPException(400, "At least one search parameter required")
    results = search_data_tables(
        db, request.data_table_id, request.label_query, request.api_endpoint, request.limit
    )
    return {"results": results, "count": len(results)}


@router.get("/datasets")
def datasets_list() -> dict[str, Any]:
    """List all available aggregate Census datasets."""
    try:
        results = list_datasets()
        return {"datasets": results, "count": len(results)}
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.post("/geography/levels")
def geography_levels(request: DatasetGeographyRequest, db: DBSession) -> dict[str, Any]:
    """Get available geography levels for a dataset."""
    try:
        results = fetch_dataset_geography(db, request.dataset, request.year)
        return {"geography_levels": results, "count": len(results)}
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.post("/data/fetch")
def data_fetch(request: FetchDataRequest, db: DBSession) -> dict[str, Any]:
    """Fetch aggregate data from the Census API."""
    try:
        return fetch_aggregate_data(
            dataset=request.dataset,
            year=request.year,
            variables=request.variables,
            group=request.group,
            for_clause=request.for_clause,
            in_clause=request.in_clause,
            ucgid=request.ucgid,
            predicates=request.predicates,
            descriptive=request.descriptive,
            db=db,
        )
    except (ValueError, RuntimeError) as e:
        raise HTTPException(400, str(e))


# ── Claude-powered natural language endpoint ─────────────────

@router.post("/query", response_model=CensusQueryResponse)
def census_query(request: CensusQueryRequest, db: DBSession) -> CensusQueryResponse:
    """Ask a natural language question about Census data.

    Claude autonomously decides which Census tools to call, resolves geography,
    finds the right tables, fetches data, and synthesizes a cited answer.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(503, "ANTHROPIC_API_KEY not configured")

    client = anthropic.Anthropic(api_key=api_key)
    start = time.time()

    system_prompt = (
        "You are a data analyst with access to the U.S. Census Bureau Data API. "
        "Use the available tools to answer the user's question with authoritative Census data. "
        "Typical workflow: (1) search_census_data_tables to find the right table, "
        "(2) resolve_geography_fips to get geography codes, "
        "(3) fetch_census_data to get the actual numbers. "
        "Always cite your data source. Format numbers with commas for readability."
    )

    messages: list[dict[str, Any]] = [{"role": "user", "content": request.query}]
    tool_call_log: list[dict[str, Any]] = []
    total_tokens = {"input_tokens": 0, "output_tokens": 0}

    # Agentic loop: let Claude call tools until it produces a final text answer
    max_iterations = 10
    for _ in range(max_iterations):
        response = client.messages.create(
            model="claude-sonnet-4-5-20250514",
            max_tokens=4096,
            system=system_prompt,
            tools=CENSUS_TOOLS,
            messages=messages,
        )

        total_tokens["input_tokens"] += response.usage.input_tokens
        total_tokens["output_tokens"] += response.usage.output_tokens

        # Process response blocks
        tool_use_blocks = [b for b in response.content if b.type == "tool_use"]
        text_blocks = [b for b in response.content if b.type == "text"]

        if not tool_use_blocks:
            # No more tool calls — Claude is done
            answer = "\n".join(b.text for b in text_blocks) if text_blocks else "No answer generated."
            break

        # Append assistant message with all content blocks
        messages.append({"role": "assistant", "content": response.content})

        # Execute each tool call and collect results
        tool_results = []
        for block in tool_use_blocks:
            try:
                result = execute_census_tool(block.name, block.input, db)
                tool_call_log.append({
                    "tool": block.name,
                    "input": block.input,
                    "success": True,
                })
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": json.dumps(result, default=str),
                })
            except Exception as e:
                logger.error(f"Census tool {block.name} failed: {e}")
                tool_call_log.append({
                    "tool": block.name,
                    "input": block.input,
                    "success": False,
                    "error": str(e),
                })
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": json.dumps({"error": str(e)}),
                    "is_error": True,
                })

        messages.append({"role": "user", "content": tool_results})
    else:
        answer = "Reached maximum tool iterations without a final answer."

    elapsed = int((time.time() - start) * 1000)

    return CensusQueryResponse(
        answer=answer,
        tool_calls=tool_call_log,
        token_usage=total_tokens,
        elapsed_ms=elapsed,
    )
