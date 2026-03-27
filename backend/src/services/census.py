"""Census Bureau Data API service.

Ported from: github.com/uscensusbureau/us-census-bureau-data-api-mcp
Provides direct Python tool-calling equivalents of the 5 MCP tools:
  - resolve_geography_fips
  - search_data_tables
  - list_datasets
  - fetch_dataset_geography
  - fetch_aggregate_data
"""

import hashlib
import json
import logging
import os
import re
from typing import Any
from urllib.parse import urlencode

import httpx
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

CENSUS_API_BASE = "https://api.census.gov/data"

# Datasets that are NOT aggregate and should be rejected
_MICRODATA_MARKERS = ("cfspum", "cps", "pums", "pumpr", "sipp")

_client: httpx.Client | None = None


def _get_http_client() -> httpx.Client:
    global _client
    if _client is None:
        _client = httpx.Client(timeout=30.0, verify=False)
    return _client


def _get_api_key() -> str | None:
    return os.getenv("CENSUS_API_KEY")


def _build_citation(url: str) -> str:
    api_key = _get_api_key()
    if api_key:
        url = url.replace(f"key={api_key}", "key=REDACTED")
    return f"Source: U.S. Census Bureau Data API ({url})"


def _validate_dataset(dataset: str) -> str | None:
    """Return error message if dataset is unsupported, else None."""
    lower = dataset.lower()
    if "timeseries" in lower:
        return "Timeseries datasets are not supported. Use aggregate datasets instead."
    for marker in _MICRODATA_MARKERS:
        if marker in lower:
            return f"Microdata dataset '{dataset}' is not supported. Use aggregate datasets instead."
    return None


# ============================================================
# Tool 1: Resolve Geography FIPS
# ============================================================

def resolve_geography_fips(
    db: Session,
    geography_name: str,
    summary_level: str | None = None,
) -> list[dict[str, Any]]:
    """Search for geographies by name with optional summary level filter.

    Returns list of matches with name, summary_level_name, lat/lon, for_param, in_param, score.
    """
    if summary_level:
        # Try to resolve the summary level first
        rows = db.execute(
            text("SELECT code, name FROM search_census_summary_levels(:term, 1)"),
            {"term": summary_level},
        ).fetchall()

        if rows:
            level_code = rows[0].code
            results = db.execute(
                text(
                    "SELECT id, name, summary_level_name, latitude, longitude, "
                    "for_param, in_param, weighted_score "
                    "FROM search_census_geographies_by_summary_level(:term, :code, 10)"
                ),
                {"term": geography_name, "code": level_code},
            ).fetchall()
        else:
            # Summary level not found — fall through to unfiltered search
            results = db.execute(
                text(
                    "SELECT id, name, summary_level_name, latitude, longitude, "
                    "for_param, in_param, weighted_score "
                    "FROM search_census_geographies(:term, 10)"
                ),
                {"term": geography_name},
            ).fetchall()
    else:
        results = db.execute(
            text(
                "SELECT id, name, summary_level_name, latitude, longitude, "
                "for_param, in_param, weighted_score "
                "FROM search_census_geographies(:term, 10)"
            ),
            {"term": geography_name},
        ).fetchall()

    if not results:
        return []

    return [
        {
            "id": r.id,
            "name": r.name,
            "summary_level": r.summary_level_name,
            "latitude": r.latitude,
            "longitude": r.longitude,
            "for_param": r.for_param,
            "in_param": r.in_param,
            "score": round(float(r.weighted_score), 4) if r.weighted_score else 0,
        }
        for r in results
    ]


# ============================================================
# Tool 2: Search Data Tables
# ============================================================

def search_data_tables(
    db: Session,
    data_table_id: str | None = None,
    label_query: str | None = None,
    api_endpoint: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Search Census data table catalog by ID, label, or endpoint.

    At least one of data_table_id, label_query, or api_endpoint must be provided.
    """
    if not any([data_table_id, label_query, api_endpoint]):
        return []

    limit = max(1, min(limit, 100))

    results = db.execute(
        text(
            "SELECT data_table_id, label, component, datasets "
            "FROM search_census_data_tables(:tid, :lq, :ep, :lim)"
        ),
        {
            "tid": data_table_id,
            "lq": label_query,
            "ep": api_endpoint,
            "lim": limit,
        },
    ).fetchall()

    return [
        {
            "data_table_id": r.data_table_id,
            "label": r.label,
            "component": r.component,
            "datasets": r.datasets if isinstance(r.datasets, dict) else json.loads(r.datasets),
        }
        for r in results
    ]


# ============================================================
# Tool 3: List Datasets
# ============================================================

def list_datasets(api_key: str | None = None) -> list[dict[str, Any]]:
    """Fetch all available aggregate Census datasets from the live API.

    Returns list of {dataset, title, years[]}.
    """
    key = api_key or _get_api_key()
    if not key:
        raise ValueError("CENSUS_API_KEY is required to list datasets")

    url = f"{CENSUS_API_BASE}.json"
    resp = _get_http_client().get(url, params={"key": key})
    resp.raise_for_status()

    data = resp.json()
    datasets_raw = data.get("dataset", [])

    # Simplify and filter aggregate only
    simplified: list[dict[str, Any]] = []
    for d in datasets_raw:
        if not d.get("c_isAggregate"):
            continue
        c_dataset = d.get("c_dataset", "")
        if isinstance(c_dataset, list):
            c_dataset = "/".join(c_dataset)
        simplified.append({
            "dataset": c_dataset,
            "title": d.get("title", ""),
            "vintage": d.get("c_vintage"),
        })

    # Sort by dataset asc, vintage desc
    simplified.sort(key=lambda x: (x["dataset"], -(x["vintage"] or 0)))

    # Group by dataset, collect years
    grouped: dict[str, dict[str, Any]] = {}
    for item in simplified:
        ds = item["dataset"]
        if ds not in grouped:
            title = item["title"]
            # Strip vintage year from title
            if item["vintage"]:
                title = re.sub(rf"\b{item['vintage']}\b", "", title).strip().rstrip(":")
            grouped[ds] = {"dataset": ds, "title": title.strip(), "years": []}
        if item["vintage"] and item["vintage"] not in grouped[ds]["years"]:
            grouped[ds]["years"].append(item["vintage"])

    return list(grouped.values())


# ============================================================
# Tool 4: Fetch Dataset Geography
# ============================================================

def fetch_dataset_geography(
    db: Session,
    dataset: str,
    year: int | None = None,
    api_key: str | None = None,
) -> list[dict[str, Any]]:
    """Fetch available geography levels for a Census dataset.

    Enriches Census API response with local DB metadata (hierarchy, query examples).
    """
    key = api_key or _get_api_key()
    if not key:
        raise ValueError("CENSUS_API_KEY is required")

    error = _validate_dataset(dataset)
    if error:
        raise ValueError(error)

    # Build URL
    path = f"{year}/{dataset}" if year else dataset
    url = f"{CENSUS_API_BASE}/{path}/geography.json"
    resp = _get_http_client().get(url, params={"key": key})
    resp.raise_for_status()

    geo_data = resp.json()
    fips_list = geo_data.get("fips", [])

    # Load summary levels from DB for enrichment
    sl_rows = db.execute(
        text(
            "SELECT code, name, description, get_variable, query_name, on_spine, "
            "hierarchy_level, parent_summary_level "
            "FROM census_summary_levels ORDER BY code"
        )
    ).fetchall()

    sl_map = {r.code: r for r in sl_rows}

    results = []
    for entry in fips_list:
        geo_level = entry.get("geoLevelDisplay", "")
        code = geo_level.zfill(3)
        sl = sl_map.get(code)

        result: dict[str, Any] = {
            "name": entry.get("name", ""),
            "code": code,
            "referenceDate": entry.get("referenceDate"),
            "requires": entry.get("requires", []),
            "wildcard": entry.get("wildcard", []),
        }

        if sl:
            result["displayName"] = sl.name
            result["description"] = sl.description
            result["onSpine"] = sl.on_spine
            result["hierarchyLevel"] = sl.hierarchy_level

            # Build query example
            if sl.parent_summary_level:
                parent = sl_map.get(sl.parent_summary_level)
                if parent and parent.code == "010":
                    result["queryExample"] = f"for={sl.query_name}:*"
                elif parent:
                    result["queryExample"] = (
                        f"for={sl.query_name}:*&in={parent.query_name}:*"
                    )
                else:
                    result["queryExample"] = f"for={sl.query_name}:*"
            else:
                result["queryExample"] = f"for={sl.query_name}:*" if sl.query_name else None

        results.append(result)

    return results


# ============================================================
# Tool 5: Fetch Aggregate Data (core)
# ============================================================

def fetch_aggregate_data(
    dataset: str,
    year: int,
    variables: list[str] | None = None,
    group: str | None = None,
    for_clause: str | None = None,
    in_clause: str | None = None,
    ucgid: str | None = None,
    predicates: dict[str, str] | None = None,
    descriptive: bool = False,
    api_key: str | None = None,
    db: Session | None = None,
) -> dict[str, Any]:
    """Fetch aggregate statistical data from the Census API.

    Args:
        dataset: Census dataset path (e.g. "acs/acs1")
        year: Data year
        variables: Variable codes (e.g. ["B01001_001E"]), max 50
        group: Table group ID (e.g. "B01001")
        for_clause: Geography "for" param (e.g. "state:06")
        in_clause: Geography "in" param (e.g. "state:06")
        ucgid: Unique Census Geography ID
        predicates: Additional predicate filters
        descriptive: Include variable labels
        api_key: Override API key
        db: Optional DB session for caching

    Returns:
        {data: [...], citation: str, rows: int}
    """
    key = api_key or _get_api_key()
    if not key:
        raise ValueError("CENSUS_API_KEY is required")

    error = _validate_dataset(dataset)
    if error:
        raise ValueError(error)

    # Validate geography: exactly one of for_clause or ucgid
    if not for_clause and not ucgid:
        raise ValueError("Either 'for_clause' or 'ucgid' must be provided")
    if for_clause and ucgid:
        raise ValueError("Provide either 'for_clause' or 'ucgid', not both")

    # Validate for_clause format
    if for_clause and not re.match(r"^[a-zA-Z+\s]+:[*\d,]+$", for_clause):
        raise ValueError(f"Invalid 'for' format: {for_clause}")
    if in_clause and not re.match(r"^[a-zA-Z]+:[*\d,]+(?:\+[a-zA-Z]+:[*\d,]+)*$", in_clause):
        raise ValueError(f"Invalid 'in' format: {in_clause}")

    if variables and len(variables) > 50:
        raise ValueError("Maximum 50 variables per request")

    # Build get param
    get_parts = []
    if variables:
        get_parts.append(",".join(variables))
    if group:
        get_parts.append(f"group({group})")
    if not get_parts:
        raise ValueError("At least one of 'variables' or 'group' must be provided")
    get_param = ",".join(get_parts)

    # Build URL
    url = f"{CENSUS_API_BASE}/{year}/{dataset}"
    params: dict[str, str] = {
        "get": get_param,
        "key": key,
    }
    if for_clause:
        params["for"] = for_clause
    if in_clause:
        params["in"] = in_clause
    if ucgid:
        params["ucgid"] = ucgid
    if descriptive:
        params["descriptive"] = "true"
    if predicates:
        params.update(predicates)

    # Check cache
    cache_key = hashlib.sha256(
        json.dumps({"url": url, "params": {k: v for k, v in params.items() if k != "key"}},
                   sort_keys=True).encode()
    ).hexdigest()

    if db:
        cached = db.execute(
            text("SELECT response_data FROM census_data_cache WHERE request_hash = :h"),
            {"h": cache_key},
        ).fetchone()
        if cached:
            cached_data = cached.response_data
            return {
                "data": cached_data,
                "citation": _build_citation(f"{url}?{urlencode(params)}"),
                "rows": len(cached_data) - 1 if isinstance(cached_data, list) else 0,
                "cached": True,
            }

    # Make the API call
    full_url = f"{url}?{urlencode(params)}"
    resp = _get_http_client().get(full_url)

    if resp.status_code != 200:
        raise RuntimeError(f"Census API error: {resp.status_code} {resp.reason_phrase}")

    raw_data: list[list[str]] = resp.json()

    # Cache the response
    if db and raw_data:
        try:
            db.execute(
                text(
                    "INSERT INTO census_data_cache (request_hash, dataset_code, year, variables, "
                    "geography_spec, response_data) "
                    "VALUES (:h, :ds, :yr, :vars, :geo, :data) "
                    "ON CONFLICT (request_hash) DO NOTHING"
                ),
                {
                    "h": cache_key,
                    "ds": dataset,
                    "yr": year,
                    "vars": variables or [],
                    "geo": json.dumps({"for": for_clause, "in": in_clause, "ucgid": ucgid}),
                    "data": json.dumps(raw_data),
                },
            )
            db.commit()
        except Exception as e:
            logger.warning(f"Failed to cache Census response: {e}")
            db.rollback()

    # Format: first row = headers, rest = data rows
    headers = raw_data[0] if raw_data else []
    rows = raw_data[1:] if len(raw_data) > 1 else []

    formatted = []
    for row in rows:
        record = {headers[i]: row[i] for i in range(min(len(headers), len(row)))}
        formatted.append(record)

    citation = _build_citation(full_url)

    return {
        "data": formatted,
        "citation": citation,
        "rows": len(formatted),
        "cached": False,
    }


# ============================================================
# Claude tool definitions (for use with Anthropic tool_use API)
# ============================================================

CENSUS_TOOLS = [
    {
        "name": "resolve_geography_fips",
        "description": (
            "Search for U.S. Census Bureau geography names and return FIPS codes and "
            "Census API parameters. Use this to find the correct 'for' and 'in' parameters "
            "before calling fetch_census_data."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "geography_name": {
                    "type": "string",
                    "description": "Geography name to search (e.g. 'California', 'Cook County, Illinois')",
                },
                "summary_level": {
                    "type": "string",
                    "description": "Optional geographic level filter (e.g. 'State', 'County', 'Place', '040')",
                },
            },
            "required": ["geography_name"],
        },
    },
    {
        "name": "search_census_data_tables",
        "description": (
            "Search the Census data table catalog to find table IDs and which datasets/years "
            "they're available in. Use this to discover which variables or table groups to request."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "data_table_id": {
                    "type": "string",
                    "description": "Table ID prefix (e.g. 'B01001', 'S0101')",
                },
                "label_query": {
                    "type": "string",
                    "description": "Fuzzy text search on table labels (e.g. 'population by age')",
                },
                "api_endpoint": {
                    "type": "string",
                    "description": "Filter to specific endpoint (e.g. 'acs/acs1')",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results (1-100, default 20)",
                },
            },
        },
    },
    {
        "name": "list_census_datasets",
        "description": (
            "List all available aggregate Census Bureau datasets with their years. "
            "Use this to discover which datasets and vintages are available."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "fetch_census_geography",
        "description": (
            "Get available geography levels for a specific Census dataset. Returns the "
            "hierarchy of geographic levels with query syntax examples."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dataset": {
                    "type": "string",
                    "description": "Dataset path (e.g. 'acs/acs1', 'dec/pl')",
                },
                "year": {
                    "type": "integer",
                    "description": "Optional year to check geography for",
                },
            },
            "required": ["dataset"],
        },
    },
    {
        "name": "fetch_census_data",
        "description": (
            "Fetch aggregate statistical data from the U.S. Census Bureau Data API. "
            "This is the primary tool for getting population, demographic, economic, "
            "and housing statistics. Use resolve_geography_fips first to get correct "
            "geography parameters, and search_census_data_tables to find table IDs."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dataset": {
                    "type": "string",
                    "description": "Dataset path (e.g. 'acs/acs1')",
                },
                "year": {
                    "type": "integer",
                    "description": "Data year (e.g. 2022)",
                },
                "variables": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Variable codes to fetch (max 50), e.g. ['B01001_001E']",
                },
                "group": {
                    "type": "string",
                    "description": "Table group ID (e.g. 'B01001') — alternative to listing variables",
                },
                "for_clause": {
                    "type": "string",
                    "description": "Geography 'for' param (e.g. 'state:06', 'county:*')",
                },
                "in_clause": {
                    "type": "string",
                    "description": "Geography 'in' param (e.g. 'state:06')",
                },
                "ucgid": {
                    "type": "string",
                    "description": "Unique Census Geography ID (alternative to for/in)",
                },
                "predicates": {
                    "type": "object",
                    "description": "Additional predicate filters (e.g. {\"NAICS2017\": \"31-33\"})",
                },
                "descriptive": {
                    "type": "boolean",
                    "description": "Include variable labels (default false)",
                },
            },
            "required": ["dataset", "year"],
        },
    },
]


def execute_census_tool(
    tool_name: str,
    tool_input: dict[str, Any],
    db: Session,
) -> dict[str, Any]:
    """Execute a Census tool by name. Used as the dispatch function for Claude tool_use."""
    if tool_name == "resolve_geography_fips":
        results = resolve_geography_fips(
            db,
            geography_name=tool_input["geography_name"],
            summary_level=tool_input.get("summary_level"),
        )
        return {"results": results} if results else {"message": "No geographies found"}

    elif tool_name == "search_census_data_tables":
        results = search_data_tables(
            db,
            data_table_id=tool_input.get("data_table_id"),
            label_query=tool_input.get("label_query"),
            api_endpoint=tool_input.get("api_endpoint"),
            limit=tool_input.get("limit", 20),
        )
        return {"results": results} if results else {"message": "No data tables found"}

    elif tool_name == "list_census_datasets":
        return {"datasets": list_datasets()}

    elif tool_name == "fetch_census_geography":
        results = fetch_dataset_geography(
            db,
            dataset=tool_input["dataset"],
            year=tool_input.get("year"),
        )
        return {"geography_levels": results}

    elif tool_name == "fetch_census_data":
        return fetch_aggregate_data(
            dataset=tool_input["dataset"],
            year=tool_input["year"],
            variables=tool_input.get("variables"),
            group=tool_input.get("group"),
            for_clause=tool_input.get("for_clause"),
            in_clause=tool_input.get("in_clause"),
            ucgid=tool_input.get("ucgid"),
            predicates=tool_input.get("predicates"),
            descriptive=tool_input.get("descriptive", False),
            db=db,
        )

    else:
        raise ValueError(f"Unknown Census tool: {tool_name}")
