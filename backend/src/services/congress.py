"""Congress.gov API service.

Provides direct Python tool-calling equivalents for querying U.S. Congressional
data: bills, members, amendments, CRS reports, and committee activity.

API docs: https://github.com/LibraryOfCongress/api.congress.gov
Base URL: https://api.congress.gov/v3/
Auth: x-api-key header (free signup at https://api.congress.gov/sign-up/)
Rate limit: 5,000 requests/hour
"""

import logging
import os
from typing import Any

import httpx

logger = logging.getLogger(__name__)

CONGRESS_API_BASE = "https://api.congress.gov/v3"

# Current congress number (118th: 2023-2025, 119th: 2025-2027)
CURRENT_CONGRESS = 119

# Judiciary committee codes (handle most immigration legislation)
JUDICIARY_COMMITTEES = {
    "house": "hsju00",
    "senate": "ssju00",
}

_client: httpx.Client | None = None


def _get_http_client() -> httpx.Client:
    global _client
    if _client is None:
        _client = httpx.Client(timeout=30.0, verify=False)
    return _client


def _get_api_key() -> str | None:
    return os.getenv("CONGRESS_API_KEY")


def _api_get(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    """Make an authenticated GET request to the Congress.gov API."""
    key = _get_api_key()
    if not key:
        raise ValueError("CONGRESS_API_KEY is required")

    url = f"{CONGRESS_API_BASE}/{path.lstrip('/')}"
    headers = {"x-api-key": key}
    all_params = {"format": "json"}
    if params:
        all_params.update(params)

    resp = _get_http_client().get(url, headers=headers, params=all_params)
    if resp.status_code != 200:
        raise RuntimeError(f"Congress API error: {resp.status_code} {resp.reason_phrase}")

    return resp.json()


def _build_citation(path: str) -> str:
    return f"Source: Congress.gov API (https://api.congress.gov/v3/{path.lstrip('/')})"


# ============================================================
# Tool 1: Search Immigration Bills
# ============================================================

def search_immigration_bills(
    congress: int | None = None,
    limit: int = 20,
    offset: int = 0,
    from_date: str | None = None,
    to_date: str | None = None,
) -> dict[str, Any]:
    """Find immigration-related bills via Judiciary committee referrals.

    Since the API has no keyword search, we use committee-based discovery
    (House & Senate Judiciary committees handle most immigration bills).
    """
    congress = congress or CURRENT_CONGRESS
    limit = max(1, min(limit, 50))

    all_bills: list[dict[str, Any]] = []

    for chamber, code in JUDICIARY_COMMITTEES.items():
        try:
            params: dict[str, Any] = {"limit": limit, "offset": offset}
            if from_date:
                params["fromDateTime"] = from_date
            if to_date:
                params["toDateTime"] = to_date

            data = _api_get(f"committee/{chamber}/{code}/bills", params)
            bills = data.get("committee-bills", {}).get("bills", [])
            if not bills:
                bills = data.get("bills", [])

            for bill in bills:
                all_bills.append({
                    "congress": bill.get("congress", congress),
                    "type": bill.get("type", ""),
                    "number": bill.get("number", ""),
                    "title": bill.get("title", ""),
                    "chamber": chamber.title(),
                    "latest_action": bill.get("latestAction", {}),
                    "update_date": bill.get("updateDate", ""),
                    "url": bill.get("url", ""),
                })
        except Exception as e:
            logger.warning(f"[congress] Failed to fetch {chamber} Judiciary bills: {e}")

    # Sort by update date descending
    all_bills.sort(key=lambda b: b.get("update_date", ""), reverse=True)

    return {
        "bills": all_bills[:limit],
        "total": len(all_bills),
        "congress": congress,
        "citation": _build_citation(f"committee/house/{JUDICIARY_COMMITTEES['house']}/bills"),
    }


# ============================================================
# Tool 2: Get Bill Details
# ============================================================

def get_bill_details(
    congress: int,
    bill_type: str,
    bill_number: int,
) -> dict[str, Any]:
    """Get full details for a specific bill including summary and actions."""
    bt = bill_type.lower()

    # Fetch bill detail
    bill_data = _api_get(f"bill/{congress}/{bt}/{bill_number}")
    bill = bill_data.get("bill", {})

    # Fetch summaries
    summaries = []
    try:
        sum_data = _api_get(f"bill/{congress}/{bt}/{bill_number}/summaries")
        for s in sum_data.get("summaries", []):
            summaries.append({
                "version": s.get("versionCode", ""),
                "action_date": s.get("actionDate", ""),
                "text": s.get("text", ""),
                "action_desc": s.get("actionDesc", ""),
            })
    except Exception:
        pass

    # Fetch actions (latest 10)
    actions = []
    try:
        act_data = _api_get(f"bill/{congress}/{bt}/{bill_number}/actions", {"limit": 10})
        for a in act_data.get("actions", []):
            actions.append({
                "date": a.get("actionDate", ""),
                "text": a.get("text", ""),
                "type": a.get("type", ""),
                "chamber": a.get("actionCode", ""),
            })
    except Exception:
        pass

    # Fetch subjects
    subjects = []
    policy_area = None
    try:
        subj_data = _api_get(f"bill/{congress}/{bt}/{bill_number}/subjects")
        subj_obj = subj_data.get("subjects", {})
        policy_area = subj_obj.get("policyArea", {}).get("name")
        for ls in subj_obj.get("legislativeSubjects", []):
            subjects.append(ls.get("name", ""))
    except Exception:
        pass

    return {
        "congress": bill.get("congress"),
        "type": bill.get("type", bt.upper()),
        "number": bill.get("number", bill_number),
        "title": bill.get("title", ""),
        "introduced_date": bill.get("introducedDate", ""),
        "origin_chamber": bill.get("originChamber", ""),
        "policy_area": policy_area or bill.get("policyArea", {}).get("name"),
        "subjects": subjects,
        "sponsors": [
            {
                "name": s.get("fullName") or f"{s.get('firstName', '')} {s.get('lastName', '')}",
                "party": s.get("party", ""),
                "state": s.get("state", ""),
            }
            for s in (bill.get("sponsors", []) if isinstance(bill.get("sponsors"), list) else [])
        ],
        "cosponsors_count": bill.get("cosponsors", {}).get("count", 0),
        "latest_action": bill.get("latestAction", {}),
        "summaries": summaries,
        "actions": actions,
        "citation": _build_citation(f"bill/{congress}/{bt}/{bill_number}"),
    }


# ============================================================
# Tool 3: Get Bill Text
# ============================================================

def get_bill_text(
    congress: int,
    bill_type: str,
    bill_number: int,
) -> dict[str, Any]:
    """Get available text versions for a bill (links to PDF, HTML, XML)."""
    bt = bill_type.lower()
    data = _api_get(f"bill/{congress}/{bt}/{bill_number}/text")
    text_versions = data.get("textVersions", [])

    versions = []
    for tv in text_versions:
        formats = []
        for fmt in tv.get("formats", []):
            formats.append({
                "type": fmt.get("type", ""),
                "url": fmt.get("url", ""),
            })
        versions.append({
            "date": tv.get("date", ""),
            "type": tv.get("type", ""),
            "formats": formats,
        })

    return {
        "bill": f"{bt.upper()} {bill_number}",
        "congress": congress,
        "versions": versions,
        "citation": _build_citation(f"bill/{congress}/{bt}/{bill_number}/text"),
    }


# ============================================================
# Tool 4: List Members
# ============================================================

def list_members(
    state: str | None = None,
    congress: int | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    """List congressional members, optionally filtered by state or congress."""
    congress = congress or CURRENT_CONGRESS
    params: dict[str, Any] = {"limit": min(limit, 250)}

    if state:
        path = f"member/{state.upper()}"
    else:
        path = f"member/congress/{congress}"
        params["currentMember"] = "true"

    data = _api_get(path, params)
    members_raw = data.get("members", [])

    members = []
    for m in members_raw:
        terms = m.get("terms", {}).get("item", []) if isinstance(m.get("terms"), dict) else []
        latest_term = terms[0] if terms else {}

        members.append({
            "bioguide_id": m.get("bioguideId", ""),
            "name": m.get("name", ""),
            "party": m.get("partyName", "") or latest_term.get("party", ""),
            "state": m.get("state", "") or latest_term.get("state", ""),
            "chamber": latest_term.get("chamber", ""),
            "district": m.get("district") or latest_term.get("district"),
        })

    return {
        "members": members,
        "total": len(members),
        "citation": _build_citation(path),
    }


# ============================================================
# Tool 5: Get Member Legislation
# ============================================================

def get_member_legislation(
    bioguide_id: str,
    legislation_type: str = "sponsored",
    limit: int = 20,
) -> dict[str, Any]:
    """Get bills sponsored or cosponsored by a specific member."""
    lt = "cosponsored-legislation" if legislation_type == "cosponsored" else "sponsored-legislation"
    params: dict[str, Any] = {"limit": min(limit, 50)}

    data = _api_get(f"member/{bioguide_id}/{lt}", params)
    bills_raw = data.get("sponsoredLegislation", []) or data.get("cosponsoredLegislation", [])

    bills = []
    for b in bills_raw:
        bills.append({
            "congress": b.get("congress", ""),
            "type": b.get("type", ""),
            "number": b.get("number", ""),
            "title": b.get("title", ""),
            "introduced_date": b.get("introducedDate", ""),
            "latest_action": b.get("latestAction", {}),
            "policy_area": b.get("policyArea", {}).get("name") if isinstance(b.get("policyArea"), dict) else None,
        })

    return {
        "bioguide_id": bioguide_id,
        "legislation_type": legislation_type,
        "bills": bills,
        "total": len(bills),
        "citation": _build_citation(f"member/{bioguide_id}/{lt}"),
    }


# ============================================================
# Tool 6: Search CRS Reports
# ============================================================

def search_crs_reports(
    limit: int = 20,
    offset: int = 0,
    from_date: str | None = None,
    to_date: str | None = None,
) -> dict[str, Any]:
    """List Congressional Research Service reports."""
    params: dict[str, Any] = {"limit": min(limit, 50), "offset": offset}
    if from_date:
        params["fromDateTime"] = from_date
    if to_date:
        params["toDateTime"] = to_date

    data = _api_get("crsreport", params)
    reports_raw = data.get("CRSReports", []) or data.get("reports", [])

    reports = []
    for r in reports_raw:
        reports.append({
            "report_number": r.get("reportNumber", ""),
            "title": r.get("title", ""),
            "update_date": r.get("updateDate", ""),
            "url": r.get("url", ""),
        })

    return {
        "reports": reports,
        "total": len(reports),
        "citation": _build_citation("crsreport"),
    }


# ============================================================
# Claude tool definitions
# ============================================================

CONGRESS_TOOLS = [
    {
        "name": "search_immigration_bills",
        "description": (
            "Search for immigration-related bills in the U.S. Congress. Finds bills "
            "referred to the House and Senate Judiciary committees, which handle the "
            "majority of immigration legislation. Returns bill titles, sponsors, latest "
            "actions, and status."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "congress": {
                    "type": "integer",
                    "description": f"Congress number (e.g. 118, 119). Default: {CURRENT_CONGRESS}",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results (1-50, default 20)",
                },
                "from_date": {
                    "type": "string",
                    "description": "Filter by update date start (YYYY-MM-DDT00:00:00Z)",
                },
                "to_date": {
                    "type": "string",
                    "description": "Filter by update date end (YYYY-MM-DDT00:00:00Z)",
                },
            },
        },
    },
    {
        "name": "get_bill_details",
        "description": (
            "Get full details for a specific Congressional bill including CRS summary, "
            "sponsors, cosponsors, legislative actions, subjects, and policy area. "
            "Use this after finding a bill via search_immigration_bills."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "congress": {"type": "integer", "description": "Congress number (e.g. 118)"},
                "bill_type": {"type": "string", "description": "Bill type: hr, s, hjres, sjres, hconres, sconres, hres, sres"},
                "bill_number": {"type": "integer", "description": "Bill number"},
            },
            "required": ["congress", "bill_type", "bill_number"],
        },
    },
    {
        "name": "get_bill_text",
        "description": (
            "Get available text versions of a bill (PDF, HTML, XML links). "
            "Returns all published versions with dates."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "congress": {"type": "integer", "description": "Congress number"},
                "bill_type": {"type": "string", "description": "Bill type (hr, s, etc.)"},
                "bill_number": {"type": "integer", "description": "Bill number"},
            },
            "required": ["congress", "bill_type", "bill_number"],
        },
    },
    {
        "name": "list_congress_members",
        "description": (
            "List members of Congress, optionally filtered by state. "
            "Returns name, party, state, chamber, and district."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "state": {"type": "string", "description": "Two-letter state code (e.g. 'CA', 'TX')"},
                "congress": {"type": "integer", "description": f"Congress number. Default: {CURRENT_CONGRESS}"},
                "limit": {"type": "integer", "description": "Max results (default 20)"},
            },
        },
    },
    {
        "name": "get_member_legislation",
        "description": (
            "Get bills sponsored or cosponsored by a specific member of Congress. "
            "Use list_congress_members first to find the member's bioguide ID."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "bioguide_id": {"type": "string", "description": "Member bioguide ID (e.g. 'D000096')"},
                "legislation_type": {"type": "string", "enum": ["sponsored", "cosponsored"], "description": "Default: sponsored"},
                "limit": {"type": "integer", "description": "Max results (default 20)"},
            },
            "required": ["bioguide_id"],
        },
    },
    {
        "name": "search_crs_reports",
        "description": (
            "Search Congressional Research Service (CRS) reports. CRS provides "
            "non-partisan policy analysis on all legislative topics. Returns report "
            "titles, numbers, and links."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "description": "Max results (default 20)"},
                "from_date": {"type": "string", "description": "Filter by date start (YYYY-MM-DDT00:00:00Z)"},
                "to_date": {"type": "string", "description": "Filter by date end (YYYY-MM-DDT00:00:00Z)"},
            },
        },
    },
]


def execute_congress_tool(
    tool_name: str,
    tool_input: dict[str, Any],
) -> dict[str, Any]:
    """Execute a Congress tool by name. Used as the dispatch function for Claude tool_use."""
    if tool_name == "search_immigration_bills":
        return search_immigration_bills(
            congress=tool_input.get("congress"),
            limit=tool_input.get("limit", 20),
            from_date=tool_input.get("from_date"),
            to_date=tool_input.get("to_date"),
        )
    elif tool_name == "get_bill_details":
        return get_bill_details(
            congress=tool_input["congress"],
            bill_type=tool_input["bill_type"],
            bill_number=tool_input["bill_number"],
        )
    elif tool_name == "get_bill_text":
        return get_bill_text(
            congress=tool_input["congress"],
            bill_type=tool_input["bill_type"],
            bill_number=tool_input["bill_number"],
        )
    elif tool_name == "list_congress_members":
        return list_members(
            state=tool_input.get("state"),
            congress=tool_input.get("congress"),
            limit=tool_input.get("limit", 20),
        )
    elif tool_name == "get_member_legislation":
        return get_member_legislation(
            bioguide_id=tool_input["bioguide_id"],
            legislation_type=tool_input.get("legislation_type", "sponsored"),
            limit=tool_input.get("limit", 20),
        )
    elif tool_name == "search_crs_reports":
        return search_crs_reports(
            limit=tool_input.get("limit", 20),
            from_date=tool_input.get("from_date"),
            to_date=tool_input.get("to_date"),
        )
    else:
        raise ValueError(f"Unknown Congress tool: {tool_name}")
