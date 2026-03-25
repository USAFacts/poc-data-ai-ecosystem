"""Graph API routes — query the Neo4j knowledge graph."""

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

router = APIRouter()


class EntityResult(BaseModel):
    name: str
    type: str


class DocumentResult(BaseModel):
    doc_id: str
    title: str | None = None
    asset: str | None = None
    agency: str | None = None
    confidence: float | None = None
    shared_entities: int | None = None


class DocumentContext(BaseModel):
    doc_id: str
    entities: list[dict[str, Any]]
    related_documents: list[dict[str, Any]]
    time_periods: list[dict[str, Any]]
    agency: dict[str, Any] | None


class GraphStats(BaseModel):
    documents: int
    entities: int
    agencies: int
    time_periods: int
    relationships: int


@router.get("/entity/{name}/related", response_model=list[EntityResult])
def get_related_entities(
    name: str,
    depth: int = Query(2, ge=1, le=4),
    limit: int = Query(20, ge=1, le=100),
):
    """Find entities related to the given entity via graph traversal."""
    try:
        from src.services.neo4j_client import find_related_entities

        return find_related_entities(name, depth=depth, limit=limit)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Graph unavailable: {e}")


@router.get("/entity/{name}/documents", response_model=list[DocumentResult])
def get_entity_documents(
    name: str,
    limit: int = Query(20, ge=1, le=100),
):
    """Find documents that mention a specific entity."""
    try:
        from src.services.neo4j_client import find_documents_by_entity

        return find_documents_by_entity(name, limit=limit)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Graph unavailable: {e}")


@router.get("/document/{doc_id:path}/context", response_model=DocumentContext)
def get_document_context(doc_id: str):
    """Get the graph neighborhood of a document (entities, related docs, time periods)."""
    try:
        from src.services.neo4j_client import find_document_context

        return find_document_context(doc_id)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Graph unavailable: {e}")


@router.get("/stats", response_model=GraphStats)
def get_graph_stats():
    """Get summary statistics about the knowledge graph."""
    try:
        from src.services.neo4j_client import get_driver

        driver = get_driver()
        with driver.session() as session:
            docs = session.run("MATCH (d:Document) RETURN count(d) AS c").single()["c"]
            entities = session.run("MATCH (e:Entity) RETURN count(e) AS c").single()["c"]
            agencies = session.run("MATCH (a:Agency) RETURN count(a) AS c").single()["c"]
            periods = session.run("MATCH (t:TimePeriod) RETURN count(t) AS c").single()["c"]
            rels = session.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]
        return GraphStats(
            documents=docs,
            entities=entities,
            agencies=agencies,
            time_periods=periods,
            relationships=rels,
        )
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Graph unavailable: {e}")
