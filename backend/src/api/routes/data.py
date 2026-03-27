"""Data layer API routes — unified access to documents, entities, and graph data."""

import io
import json
import logging
import mimetypes
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from src.api.deps import DBSession, Storage

logger = logging.getLogger(__name__)

router = APIRouter()

# ---------------------------------------------------------------------------
# Pydantic response models
# ---------------------------------------------------------------------------


class DocumentSummary(BaseModel):
    doc_id: str
    title: str | None = None
    agency: str | None = None
    asset: str | None = None
    document_type: str | None = None
    date_str: str | None = None
    key_topics: list[str] = []
    entity_names: list[str] = []
    summary: str | None = None
    storage_path: str | None = None
    score: float | None = None


class DocumentListResponse(BaseModel):
    total: int
    offset: int
    limit: int
    documents: list[DocumentSummary]


class DocumentDetail(BaseModel):
    metadata: dict[str, Any] = {}
    source: dict[str, Any] = {}
    enrichment: dict[str, Any] = {}
    content_sections: list[dict[str, Any]] = []
    tables: list[dict[str, Any]] = []
    chunk_count: int = 0


class ChunkItem(BaseModel):
    chunk_id: str = ""
    level: str = ""
    sequence: int = 0
    title: str | None = None
    text: str = ""
    page_number: int = 0


class TableItem(BaseModel):
    title: str | None = None
    headers: list[str] = []
    rows: list[list[Any]] = []
    page_number: int | None = None
    sheet_name: str | None = None


class EntitySummary(BaseModel):
    canonical_name: str
    type: str
    aliases: list[str] = []
    mention_count: int = 0


class EntityDetail(BaseModel):
    canonical_name: str
    type: str
    aliases: list[str] = []
    mention_count: int = 0
    documents: list[dict[str, Any]] = []
    related_entities: list[dict[str, Any]] = []


class TimePeriodItem(BaseModel):
    period: str
    start_date: str | None = None
    end_date: str | None = None
    document_count: int = 0


class ObjectInfo(BaseModel):
    object_name: str
    size: int | None = None
    last_modified: str | None = None
    content_type: str | None = None


class CoverageStat(BaseModel):
    documents_per_agency: dict[str, int] = {}
    documents_per_format: dict[str, int] = {}
    topic_distribution: dict[str, int] = {}
    entity_type_distribution: dict[str, int] = {}


class FreshnessItem(BaseModel):
    asset: str
    agency: str | None = None
    last_modified: str | None = None
    date_str: str | None = None


class GraphPathNode(BaseModel):
    name: str
    type: str
    labels: list[str] = []


class GraphPathRelationship(BaseModel):
    type: str
    start_name: str
    end_name: str


class GraphPathResult(BaseModel):
    nodes: list[GraphPathNode]
    relationships: list[GraphPathRelationship]


# ---------------------------------------------------------------------------
# Helper: Weaviate filter builder
# ---------------------------------------------------------------------------

def _build_weaviate_filter(agency: str | None = None, format: str | None = None):
    """Build a Weaviate v4 filter from optional parameters."""
    from weaviate.classes.query import Filter

    conditions = []
    if agency:
        conditions.append(Filter.by_property("agency").equal(agency))
    if format:
        conditions.append(Filter.by_property("document_type").equal(format))

    if len(conditions) == 0:
        return None
    if len(conditions) == 1:
        return conditions[0]
    return Filter.all_of(conditions)


# ===========================================================================
# DOCUMENTS
# ===========================================================================


@router.get("/documents/search", response_model=list[DocumentSummary])
def search_documents_structured(
    storage: Storage,
    q: str = Query(..., min_length=1),
    agency: str | None = Query(None),
    limit: int = Query(10, ge=1, le=50),
):
    """Structured BM25 search on GovDocument collection."""
    try:
        from weaviate.classes.query import Filter, MetadataQuery

        from src.services.weaviate_client import GOV_DOCUMENT_COLLECTION, get_client

        client = get_client()
        collection = client.collections.get(GOV_DOCUMENT_COLLECTION)

        weaviate_filter = None
        if agency:
            weaviate_filter = Filter.by_property("agency").equal(agency)

        results = collection.query.bm25(
            query=q,
            limit=limit,
            filters=weaviate_filter,
            return_metadata=MetadataQuery(score=True),
        )

        output: list[DocumentSummary] = []
        for obj in results.objects:
            props = dict(obj.properties)
            output.append(DocumentSummary(
                doc_id=props.get("doc_id", ""),
                title=props.get("title"),
                agency=props.get("agency"),
                asset=props.get("asset"),
                document_type=props.get("document_type"),
                date_str=props.get("date_str"),
                key_topics=props.get("key_topics") or [],
                entity_names=props.get("entity_names") or [],
                summary=props.get("summary"),
                storage_path=props.get("storage_path"),
                score=obj.metadata.score if obj.metadata else None,
            ))
        return output
    except Exception as e:
        logger.exception("search_documents_structured failed")
        raise HTTPException(status_code=503, detail=f"Weaviate unavailable: {e}")


@router.get("/documents/{doc_id:path}/chunks", response_model=list[ChunkItem])
def get_document_chunks(
    doc_id: str,
    level: str | None = Query(None),
):
    """Get all chunks for a document from Weaviate GovChunk collection."""
    try:
        from weaviate.classes.query import Filter

        from src.services.weaviate_client import GOV_CHUNK_COLLECTION, get_client

        client = get_client()
        collection = client.collections.get(GOV_CHUNK_COLLECTION)

        conditions = [Filter.by_property("doc_id").equal(doc_id)]
        if level:
            conditions.append(Filter.by_property("level").equal(level))

        weaviate_filter = conditions[0] if len(conditions) == 1 else Filter.all_of(conditions)

        results = collection.query.fetch_objects(
            filters=weaviate_filter,
            limit=500,
        )

        chunks: list[ChunkItem] = []
        for obj in results.objects:
            props = dict(obj.properties)
            chunks.append(ChunkItem(
                chunk_id=props.get("chunk_id", ""),
                level=props.get("level", ""),
                sequence=props.get("sequence", 0),
                title=props.get("title"),
                text=props.get("text", ""),
                page_number=props.get("page_number", 0),
            ))

        chunks.sort(key=lambda c: c.sequence)
        return chunks
    except Exception as e:
        logger.exception("get_document_chunks failed")
        raise HTTPException(status_code=503, detail=f"Weaviate unavailable: {e}")


@router.get("/documents/{doc_id:path}/tables", response_model=list[TableItem])
def get_document_tables(doc_id: str, storage: Storage):
    """Get tables from the enriched document in MinIO."""
    try:
        # Find the enriched doc in MinIO
        enriched_doc = _load_enriched_doc(storage, doc_id)
        if enriched_doc is None:
            raise HTTPException(status_code=404, detail="Document not found in enrichment zone")

        tables: list[TableItem] = []
        content = enriched_doc.get("content", {})
        raw_tables = content.get("tables", [])
        for t in raw_tables:
            tables.append(TableItem(
                title=t.get("title"),
                headers=t.get("headers", []),
                rows=t.get("rows", []),
                page_number=t.get("page_number"),
                sheet_name=t.get("sheet_name"),
            ))
        return tables
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("get_document_tables failed")
        raise HTTPException(status_code=503, detail=f"Storage unavailable: {e}")


@router.get("/documents/{doc_id:path}", response_model=DocumentDetail)
def get_document(doc_id: str, storage: Storage):
    """Get a single document's full content from MinIO enrichment zone."""
    try:
        enriched_doc = _load_enriched_doc(storage, doc_id)
        if enriched_doc is None:
            raise HTTPException(status_code=404, detail="Document not found in enrichment zone")

        # Count chunks from Weaviate
        chunk_count = 0
        try:
            from weaviate.classes.query import AggregateInteger, Filter

            from src.services.weaviate_client import GOV_CHUNK_COLLECTION, get_client

            client = get_client()
            collection = client.collections.get(GOV_CHUNK_COLLECTION)
            result = collection.query.fetch_objects(
                filters=Filter.by_property("doc_id").equal(doc_id),
                limit=1000,
            )
            chunk_count = len(result.objects)
        except Exception:
            pass

        content = enriched_doc.get("content", {})
        return DocumentDetail(
            metadata=enriched_doc.get("metadata", {}),
            source=enriched_doc.get("source", {}),
            enrichment=enriched_doc.get("enrichment", {}),
            content_sections=content.get("sections", []),
            tables=content.get("tables", []),
            chunk_count=chunk_count,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("get_document failed")
        raise HTTPException(status_code=503, detail=f"Storage unavailable: {e}")


@router.get("/documents", response_model=DocumentListResponse)
def list_documents(
    storage: Storage,
    agency: str | None = Query(None),
    format: str | None = Query(None),
    search: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """List documents from Weaviate GovDocument collection with optional filters."""
    try:
        from weaviate.classes.query import Filter, MetadataQuery

        from src.services.weaviate_client import GOV_DOCUMENT_COLLECTION, get_client

        client = get_client()
        collection = client.collections.get(GOV_DOCUMENT_COLLECTION)

        weaviate_filter = _build_weaviate_filter(agency=agency, format=format)

        if search:
            # Use BM25 when search text is provided
            results = collection.query.bm25(
                query=search,
                limit=limit + offset,
                filters=weaviate_filter,
                return_metadata=MetadataQuery(score=True),
            )
        else:
            results = collection.query.fetch_objects(
                filters=weaviate_filter,
                limit=limit + offset,
            )

        all_objects = results.objects
        # Manual offset
        paged = all_objects[offset: offset + limit]

        documents: list[DocumentSummary] = []
        for obj in paged:
            props = dict(obj.properties)
            score = None
            if search and obj.metadata:
                score = obj.metadata.score
            documents.append(DocumentSummary(
                doc_id=props.get("doc_id", ""),
                title=props.get("title"),
                agency=props.get("agency"),
                asset=props.get("asset"),
                document_type=props.get("document_type"),
                date_str=props.get("date_str"),
                key_topics=props.get("key_topics") or [],
                entity_names=props.get("entity_names") or [],
                summary=props.get("summary"),
                storage_path=props.get("storage_path"),
                score=score,
            ))

        return DocumentListResponse(
            total=len(all_objects),
            offset=offset,
            limit=limit,
            documents=documents,
        )
    except Exception as e:
        logger.exception("list_documents failed")
        raise HTTPException(status_code=503, detail=f"Weaviate unavailable: {e}")


# ===========================================================================
# ENTITIES
# ===========================================================================


@router.get("/entities", response_model=list[EntitySummary])
def list_entities(
    type: str | None = Query(None),
    search: str | None = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """List entities from Neo4j with optional type filter and text search."""
    try:
        from src.services.neo4j_client import get_driver

        driver = get_driver()
        with driver.session() as session:
            # Build dynamic WHERE clause
            where_parts: list[str] = []
            params: dict[str, Any] = {"limit": limit, "offset": offset}

            if type:
                where_parts.append("e.type = $type")
                params["type"] = type
            if search:
                where_parts.append("toLower(e.canonical_name) CONTAINS toLower($search)")
                params["search"] = search

            where_clause = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

            query = f"""
                MATCH (e:Entity)
                {where_clause}
                OPTIONAL MATCH (e)<-[r:MENTIONS]-()
                RETURN e.canonical_name AS canonical_name,
                       e.type AS type,
                       e.aliases AS aliases,
                       count(r) AS mention_count
                ORDER BY e.canonical_name
                SKIP $offset LIMIT $limit
            """
            result = session.run(query, params)
            entities: list[EntitySummary] = []
            for record in result:
                entities.append(EntitySummary(
                    canonical_name=record["canonical_name"] or "",
                    type=record["type"] or "",
                    aliases=record["aliases"] or [],
                    mention_count=record["mention_count"],
                ))
            return entities
    except Exception as e:
        logger.exception("list_entities failed")
        raise HTTPException(status_code=503, detail=f"Graph unavailable: {e}")


@router.get("/entities/{name}", response_model=EntityDetail)
def get_entity(name: str):
    """Get entity detail from Neo4j including documents and related entities."""
    try:
        from src.services.neo4j_client import get_driver

        driver = get_driver()
        with driver.session() as session:
            # Get entity info
            entity_row = session.run(
                """
                MATCH (e:Entity {canonical_name: $name})
                OPTIONAL MATCH (e)<-[r:MENTIONS]-()
                RETURN e.canonical_name AS canonical_name,
                       e.type AS type,
                       e.aliases AS aliases,
                       count(r) AS mention_count
                """,
                {"name": name},
            ).single()

            if entity_row is None:
                raise HTTPException(status_code=404, detail=f"Entity '{name}' not found")

            # Documents mentioning entity
            docs = [
                dict(r) for r in session.run(
                    """
                    MATCH (d:Document)-[r:MENTIONS]->(e:Entity {canonical_name: $name})
                    RETURN d.doc_id AS doc_id, d.title AS title,
                           d.asset AS asset, d.agency AS agency,
                           r.confidence AS confidence
                    ORDER BY r.confidence DESC
                    LIMIT 50
                    """,
                    {"name": name},
                )
            ]

            # Related entities
            related = [
                dict(r) for r in session.run(
                    """
                    MATCH (e:Entity {canonical_name: $name})-[*1..2]-(related:Entity)
                    WHERE related <> e
                    RETURN DISTINCT related.canonical_name AS canonical_name,
                           related.type AS type
                    LIMIT 30
                    """,
                    {"name": name},
                )
            ]

            return EntityDetail(
                canonical_name=entity_row["canonical_name"] or "",
                type=entity_row["type"] or "",
                aliases=entity_row["aliases"] or [],
                mention_count=entity_row["mention_count"],
                documents=docs,
                related_entities=related,
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("get_entity failed")
        raise HTTPException(status_code=503, detail=f"Graph unavailable: {e}")


# ===========================================================================
# AGENCIES
# ===========================================================================


@router.get("/agencies/{name}/documents")
def get_agency_documents(name: str):
    """Get all documents published by an agency from Neo4j."""
    try:
        from src.services.neo4j_client import get_driver

        driver = get_driver()
        with driver.session() as session:
            result = session.run(
                """
                MATCH (d:Document)-[:PUBLISHED_BY]->(a:Agency {name: $name})
                RETURN d.doc_id AS doc_id, d.title AS title,
                       d.asset AS asset, d.agency AS agency,
                       d.document_type AS document_type, d.date AS date
                ORDER BY d.date DESC
                """,
                {"name": name},
            )
            return [dict(r) for r in result]
    except Exception as e:
        logger.exception("get_agency_documents failed")
        raise HTTPException(status_code=503, detail=f"Graph unavailable: {e}")


# ===========================================================================
# TIME PERIODS
# ===========================================================================


@router.get("/time-periods", response_model=list[TimePeriodItem])
def list_time_periods():
    """List all time periods from Neo4j with document counts."""
    try:
        from src.services.neo4j_client import get_driver

        driver = get_driver()
        with driver.session() as session:
            result = session.run(
                """
                MATCH (t:TimePeriod)
                OPTIONAL MATCH (t)<-[:COVERS_PERIOD]-(d:Document)
                RETURN t.period AS period,
                       t.start_date AS start_date,
                       t.end_date AS end_date,
                       count(d) AS document_count
                ORDER BY t.period
                """
            )
            return [
                TimePeriodItem(
                    period=r["period"] or "",
                    start_date=r["start_date"],
                    end_date=r["end_date"],
                    document_count=r["document_count"],
                )
                for r in result
            ]
    except Exception as e:
        logger.exception("list_time_periods failed")
        raise HTTPException(status_code=503, detail=f"Graph unavailable: {e}")


@router.get("/time-periods/{period}/documents")
def get_period_documents(period: str):
    """Get documents covering a specific time period."""
    try:
        from src.services.neo4j_client import get_driver

        driver = get_driver()
        with driver.session() as session:
            result = session.run(
                """
                MATCH (d:Document)-[:COVERS_PERIOD]->(t:TimePeriod {period: $period})
                RETURN d.doc_id AS doc_id, d.title AS title,
                       d.asset AS asset, d.agency AS agency,
                       d.document_type AS document_type, d.date AS date
                ORDER BY d.date DESC
                """,
                {"period": period},
            )
            return [dict(r) for r in result]
    except Exception as e:
        logger.exception("get_period_documents failed")
        raise HTTPException(status_code=503, detail=f"Graph unavailable: {e}")


# ===========================================================================
# STATISTICS
# ===========================================================================


@router.get("/stats/coverage", response_model=CoverageStat)
def get_data_coverage(storage: Storage, db: DBSession):
    """Return coverage stats: docs per agency, per format, topic/entity distributions."""
    try:
        from src.services.weaviate_client import GOV_DOCUMENT_COLLECTION, get_client

        client = get_client()
        collection = client.collections.get(GOV_DOCUMENT_COLLECTION)

        results = collection.query.fetch_objects(limit=1000)

        docs_per_agency: dict[str, int] = {}
        docs_per_format: dict[str, int] = {}
        topic_dist: dict[str, int] = {}
        entity_type_dist: dict[str, int] = {}

        for obj in results.objects:
            props = dict(obj.properties)
            agency = props.get("agency") or "unknown"
            docs_per_agency[agency] = docs_per_agency.get(agency, 0) + 1

            doc_type = props.get("document_type") or "unknown"
            docs_per_format[doc_type] = docs_per_format.get(doc_type, 0) + 1

            for topic in (props.get("key_topics") or []):
                topic_dist[topic] = topic_dist.get(topic, 0) + 1

        # Entity type distribution from Neo4j
        try:
            from src.services.neo4j_client import get_driver

            driver = get_driver()
            with driver.session() as session:
                result = session.run(
                    "MATCH (e:Entity) RETURN e.type AS type, count(e) AS cnt"
                )
                for r in result:
                    entity_type_dist[r["type"] or "unknown"] = r["cnt"]
        except Exception:
            pass

        return CoverageStat(
            documents_per_agency=docs_per_agency,
            documents_per_format=docs_per_format,
            topic_distribution=topic_dist,
            entity_type_distribution=entity_type_dist,
        )
    except Exception as e:
        logger.exception("get_data_coverage failed")
        raise HTTPException(status_code=503, detail=f"Service unavailable: {e}")


@router.get("/stats/freshness", response_model=list[FreshnessItem])
def get_data_freshness(storage: Storage):
    """Return recency info per asset from Weaviate documents."""
    try:
        from src.services.weaviate_client import GOV_DOCUMENT_COLLECTION, get_client

        client = get_client()
        collection = client.collections.get(GOV_DOCUMENT_COLLECTION)

        results = collection.query.fetch_objects(limit=1000)

        # Group by asset, keep latest
        asset_map: dict[str, dict[str, Any]] = {}
        for obj in results.objects:
            props = dict(obj.properties)
            asset = props.get("asset") or ""
            if not asset:
                continue
            existing = asset_map.get(asset)
            date_str = props.get("date_str") or ""
            if existing is None or date_str > (existing.get("date_str") or ""):
                asset_map[asset] = props

        freshness: list[FreshnessItem] = []
        for asset, props in sorted(asset_map.items()):
            freshness.append(FreshnessItem(
                asset=asset,
                agency=props.get("agency"),
                date_str=props.get("date_str"),
            ))
        return freshness
    except Exception as e:
        logger.exception("get_data_freshness failed")
        raise HTTPException(status_code=503, detail=f"Service unavailable: {e}")


# ===========================================================================
# OBJECTS (MinIO raw file access)
# ===========================================================================

_ALLOWED_ZONES = {"landing-zone", "parsed-zone", "enrichment-zone", "chunk-zone"}


@router.get("/objects/list", response_model=list[ObjectInfo])
def list_objects(
    storage: Storage,
    zone: str = Query("landing-zone"),
    prefix: str = Query(""),
    limit: int = Query(100, ge=1, le=1000),
):
    """List objects in a MinIO zone."""
    if zone not in _ALLOWED_ZONES:
        raise HTTPException(status_code=400, detail=f"Zone must be one of: {', '.join(sorted(_ALLOWED_ZONES))}")

    try:
        full_prefix = f"{zone}/{prefix}" if prefix else f"{zone}/"
        raw_objects = list(
            storage.client.list_objects(storage.bucket, prefix=full_prefix, recursive=True)
        )

        items: list[ObjectInfo] = []
        for obj in raw_objects[:limit]:
            ct = mimetypes.guess_type(obj.object_name)[0] or "application/octet-stream"
            items.append(ObjectInfo(
                object_name=obj.object_name,
                size=obj.size,
                last_modified=obj.last_modified.isoformat() if obj.last_modified else None,
                content_type=ct,
            ))
        return items
    except Exception as e:
        logger.exception("list_objects failed")
        raise HTTPException(status_code=503, detail=f"Storage unavailable: {e}")


@router.get("/objects/download")
def download_object(storage: Storage, path: str = Query(...)):
    """Download a raw file from MinIO as a streaming response."""
    # Security: only allow known zones
    zone = path.split("/")[0] if "/" in path else ""
    if zone not in _ALLOWED_ZONES:
        raise HTTPException(
            status_code=403,
            detail=f"Access denied. Only these zones are accessible: {', '.join(sorted(_ALLOWED_ZONES))}",
        )

    try:
        response = storage.client.get_object(storage.bucket, path)
        content_type = mimetypes.guess_type(path)[0] or "application/octet-stream"
        filename = path.rsplit("/", 1)[-1]

        def iterfile():
            try:
                while True:
                    chunk = response.read(8192)
                    if not chunk:
                        break
                    yield chunk
            finally:
                response.close()
                response.release_conn()

        return StreamingResponse(
            iterfile(),
            media_type=content_type,
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except Exception as e:
        logger.exception("download_object failed")
        raise HTTPException(status_code=503, detail=f"Storage unavailable: {e}")


# ===========================================================================
# GRAPH — shortest path
# ===========================================================================


@router.get("/graph/paths", response_model=GraphPathResult)
def find_shortest_path(
    from_entity: str = Query(...),
    to_entity: str = Query(...),
    max_depth: int = Query(4, ge=1, le=6),
):
    """Find the shortest path between two entities in Neo4j."""
    try:
        from src.services.neo4j_client import get_driver

        driver = get_driver()
        with driver.session() as session:
            result = session.run(
                f"""
                MATCH (a {{canonical_name: $from_name}}),
                      (b {{canonical_name: $to_name}})
                MATCH path = shortestPath((a)-[*..{max_depth}]-(b))
                RETURN path
                """,
                {"from_name": from_entity, "to_name": to_entity},
            ).single()

            if result is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"No path found between '{from_entity}' and '{to_entity}' within depth {max_depth}",
                )

            path = result["path"]
            nodes: list[GraphPathNode] = []
            relationships: list[GraphPathRelationship] = []

            for node in path.nodes:
                props = dict(node)
                nodes.append(GraphPathNode(
                    name=props.get("canonical_name") or props.get("name") or props.get("period") or str(node.id),
                    type=props.get("type", ""),
                    labels=list(node.labels),
                ))

            for rel in path.relationships:
                start_props = dict(rel.start_node) if hasattr(rel, "start_node") else {}
                end_props = dict(rel.end_node) if hasattr(rel, "end_node") else {}
                start_name = start_props.get("canonical_name") or start_props.get("name") or ""
                end_name = end_props.get("canonical_name") or end_props.get("name") or ""
                relationships.append(GraphPathRelationship(
                    type=rel.type,
                    start_name=start_name,
                    end_name=end_name,
                ))

            return GraphPathResult(nodes=nodes, relationships=relationships)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("find_shortest_path failed")
        raise HTTPException(status_code=503, detail=f"Graph unavailable: {e}")


# ===========================================================================
# Helpers
# ===========================================================================


def _load_enriched_doc(storage: Storage, doc_id: str) -> dict[str, Any] | None:
    """Load an enriched document JSON from MinIO by doc_id.

    Searches enrichment-zone for files matching the doc_id, returning the
    latest version found.
    """
    prefix = "enrichment-zone/"
    try:
        all_objects = list(
            storage.client.list_objects(storage.bucket, prefix=prefix, recursive=True)
        )
    except Exception:
        return None

    # Find objects whose path contains the doc_id
    candidates = []
    for obj in all_objects:
        if not obj.object_name.endswith(".json") or obj.object_name.endswith("_metadata.json"):
            continue
        # doc_id may appear as part of the path or inside the JSON
        # Try matching by filename or path segment
        if doc_id in obj.object_name:
            candidates.append(obj)

    if not candidates:
        # Fallback: scan all enriched docs and match by metadata.identifier
        for obj in all_objects:
            if not obj.object_name.endswith(".json") or obj.object_name.endswith("_metadata.json"):
                continue
            try:
                data = storage.get_object(obj.object_name)
                if data is None:
                    continue
                doc = json.loads(data.decode("utf-8"))
                if doc.get("metadata", {}).get("identifier") == doc_id:
                    return doc
            except Exception:
                continue
        return None

    # Sort by last_modified descending to get the latest
    candidates.sort(key=lambda o: o.last_modified or "", reverse=True)
    best = candidates[0]
    data = storage.get_object(best.object_name)
    if data is None:
        return None
    try:
        return json.loads(data.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
