"""Congress.gov API routes.

Exposes Congressional data tools as REST endpoints and provides a Claude-powered
natural language interface for querying bills, members, and legislative activity.
"""

import json
import logging
import os
import time
from typing import Any

import anthropic
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from src.services.congress import (
    CONGRESS_TOOLS,
    execute_congress_tool,
    get_bill_details,
    get_bill_text,
    list_members,
    get_member_legislation,
    search_crs_reports,
    search_immigration_bills,
)

logger = logging.getLogger(__name__)

router = APIRouter()


# -- Pydantic models --

class BillSearchRequest(BaseModel):
    congress: int | None = None
    limit: int = Field(20, ge=1, le=50)
    from_date: str | None = None
    to_date: str | None = None


class BillDetailRequest(BaseModel):
    congress: int
    bill_type: str
    bill_number: int


class MemberListRequest(BaseModel):
    state: str | None = None
    congress: int | None = None
    limit: int = Field(20, ge=1, le=250)


class MemberLegislationRequest(BaseModel):
    bioguide_id: str
    legislation_type: str = "sponsored"
    limit: int = Field(20, ge=1, le=50)


class CRSReportRequest(BaseModel):
    limit: int = Field(20, ge=1, le=50)
    from_date: str | None = None
    to_date: str | None = None


class CongressQueryRequest(BaseModel):
    query: str = Field(..., description="Natural language question about Congressional legislation")


class CongressQueryResponse(BaseModel):
    answer: str
    tool_calls: list[dict[str, Any]] = []
    token_usage: dict[str, int] = {}
    elapsed_ms: int = 0


# -- Direct tool endpoints --

@router.post("/bills/immigration")
def immigration_bills(request: BillSearchRequest) -> dict[str, Any]:
    """Search immigration-related bills via Judiciary committee referrals."""
    try:
        return search_immigration_bills(
            congress=request.congress,
            limit=request.limit,
            from_date=request.from_date,
            to_date=request.to_date,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.post("/bills/detail")
def bill_detail(request: BillDetailRequest) -> dict[str, Any]:
    """Get full details for a specific bill."""
    try:
        return get_bill_details(request.congress, request.bill_type, request.bill_number)
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.post("/bills/text")
def bill_text(request: BillDetailRequest) -> dict[str, Any]:
    """Get text versions of a bill."""
    try:
        return get_bill_text(request.congress, request.bill_type, request.bill_number)
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.post("/members")
def members_list(request: MemberListRequest) -> dict[str, Any]:
    """List congressional members."""
    try:
        return list_members(state=request.state, congress=request.congress, limit=request.limit)
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.post("/members/legislation")
def member_legislation(request: MemberLegislationRequest) -> dict[str, Any]:
    """Get legislation sponsored/cosponsored by a member."""
    try:
        return get_member_legislation(
            bioguide_id=request.bioguide_id,
            legislation_type=request.legislation_type,
            limit=request.limit,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))


@router.post("/crs-reports")
def crs_reports(request: CRSReportRequest) -> dict[str, Any]:
    """List CRS research reports."""
    try:
        return search_crs_reports(
            limit=request.limit,
            from_date=request.from_date,
            to_date=request.to_date,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))


# -- Claude-powered natural language endpoint --

@router.post("/query", response_model=CongressQueryResponse)
def congress_query(request: CongressQueryRequest) -> CongressQueryResponse:
    """Ask a natural language question about Congressional legislation.

    Claude autonomously decides which Congress tools to call, fetches data,
    and synthesizes a cited answer about bills, members, and legislative activity.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise HTTPException(503, "ANTHROPIC_API_KEY not configured")

    client = anthropic.Anthropic(api_key=api_key)
    start = time.time()

    system_prompt = (
        "You are a legislative analyst with access to the Congress.gov API. "
        "Use the available tools to answer the user's question with authoritative "
        "Congressional data. Typical workflow: (1) search_immigration_bills to find "
        "relevant legislation, (2) get_bill_details for specifics on a bill, "
        "(3) list_congress_members or get_member_legislation for member info. "
        "Always cite Congress.gov as your source. Include bill numbers (e.g. H.R. 1234) "
        "and member names with party/state."
    )

    messages: list[dict[str, Any]] = [{"role": "user", "content": request.query}]
    tool_call_log: list[dict[str, Any]] = []
    total_tokens = {"input_tokens": 0, "output_tokens": 0}

    max_iterations = 10
    answer = ""
    for _ in range(max_iterations):
        response = client.messages.create(
            model="claude-sonnet-4-5-20250514",
            max_tokens=4096,
            system=system_prompt,
            tools=CONGRESS_TOOLS,
            messages=messages,
        )

        total_tokens["input_tokens"] += response.usage.input_tokens
        total_tokens["output_tokens"] += response.usage.output_tokens

        tool_use_blocks = [b for b in response.content if b.type == "tool_use"]
        text_blocks = [b for b in response.content if b.type == "text"]

        if not tool_use_blocks:
            answer = "\n".join(b.text for b in text_blocks) if text_blocks else "No answer generated."
            break

        messages.append({"role": "assistant", "content": response.content})

        tool_results = []
        for block in tool_use_blocks:
            try:
                result = execute_congress_tool(block.name, block.input)
                tool_call_log.append({"tool": block.name, "input": block.input, "success": True})
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": json.dumps(result, default=str),
                })
            except Exception as e:
                logger.error(f"Congress tool {block.name} failed: {e}")
                tool_call_log.append({"tool": block.name, "input": block.input, "success": False, "error": str(e)})
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

    return CongressQueryResponse(
        answer=answer,
        tool_calls=tool_call_log,
        token_usage=total_tokens,
        elapsed_ms=elapsed,
    )
