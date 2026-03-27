"""MCP Server for Gov Data AI Ecosystem.

Provides Claude Desktop with direct access to:
- Weaviate: semantic search across government documents and chunks
- Neo4j: knowledge graph traversal (entities, relationships, paths)
- MinIO: raw document access (PDF, XLSX, CSV files)
- PostgreSQL: pipeline metadata, assets, agencies, workflows
"""

import json
import os
from typing import Any

from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

# Load environment from the backend .env
env_path = os.path.join(os.path.dirname(__file__), "..", "..", "..", "backend", ".env")
load_dotenv(env_path)
# Also try project-level .env
load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

mcp = FastMCP(
    "Gov Data Sources",
    instructions="You have direct access to government data across 5 systems: Weaviate (vector search over 37 documents and 2000+ chunks), Neo4j (knowledge graph with entities, agencies, time periods), MinIO (raw document files — PDF, XLSX, CSV), PostgreSQL (pipeline metadata), and live web search via Firecrawl (.gov websites and USAFacts.org). Use search_documents or search_chunks for ingested data, find_entity or query_graph for relationships, read_document for full content, and search_gov_websites or search_usafacts for current information not yet ingested.",
)

# ---------------------------------------------------------------------------
# Connection helpers (lazy initialization)
# ---------------------------------------------------------------------------

_weaviate_client = None
_neo4j_driver = None
_pg_engine = None
_minio_client = None


def _get_weaviate():
    global _weaviate_client
    if _weaviate_client is None:
        import weaviate
        host = os.getenv("WEAVIATE_HOST", "localhost")
        port = int(os.getenv("WEAVIATE_PORT", "8085"))
        grpc_port = int(os.getenv("WEAVIATE_GRPC_PORT", "50051"))
        _weaviate_client = weaviate.connect_to_local(host=host, port=port, grpc_port=grpc_port)
    return _weaviate_client


def _get_neo4j():
    global _neo4j_driver
    if _neo4j_driver is None:
        from neo4j import GraphDatabase
        uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        user = os.getenv("NEO4J_USER", "neo4j")
        password = os.getenv("NEO4J_PASSWORD", "pipeline123")
        _neo4j_driver = GraphDatabase.driver(uri, auth=(user, password))
    return _neo4j_driver


def _get_minio():
    global _minio_client
    if _minio_client is None:
        from minio import Minio
        endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
        _minio_client = Minio(
            endpoint,
            access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
        )
    return _minio_client


def _get_pg_engine():
    global _pg_engine
    if _pg_engine is None:
        from sqlalchemy import create_engine
        url = os.getenv("DATABASE_URL", "postgresql://pipeline:pipeline@localhost:5432/pipeline")
        _pg_engine = create_engine(url)
    return _pg_engine


# ---------------------------------------------------------------------------
# WEAVIATE TOOLS — Semantic Search
# ---------------------------------------------------------------------------

@mcp.tool()
def search_documents(query: str, limit: int = 10) -> str:
    """Search government documents using hybrid search (BM25 + vector similarity).

    Searches across 37 ingested government documents from agencies like USCIS, DHS, OHSS.
    Returns document titles, agencies, summaries, topics, and relevance scores.

    Args:
        query: Natural language search query (e.g., "H-1B visa statistics" or "immigration backlog")
        limit: Maximum number of results (default 10, max 50)
    """
    client = _get_weaviate()
    collection = client.collections.get("GovDocument")

    results = collection.query.bm25(query=query, limit=min(limit, 50))

    docs = []
    for obj in results.objects:
        props = obj.properties
        docs.append({
            "doc_id": props.get("doc_id", ""),
            "title": props.get("title", ""),
            "agency": props.get("agency", ""),
            "asset": props.get("asset", ""),
            "summary": props.get("summary", "")[:500],
            "document_type": props.get("document_type", ""),
            "key_topics": props.get("key_topics", []),
            "temporal_period": props.get("temporal_period", ""),
            "entity_names": props.get("entity_names", [])[:10],
        })

    return json.dumps(docs, indent=2)


@mcp.tool()
def search_chunks(query: str, limit: int = 10, agency: str | None = None) -> str:
    """Search document chunks for specific content using hybrid search.

    Chunks are granular text segments (paragraphs, sections, tables) from government documents.
    More precise than document-level search for finding specific facts, numbers, or tables.

    Args:
        query: Search query (e.g., "I-130 pending backlog FY2025")
        limit: Maximum results (default 10, max 30)
        agency: Optional agency filter (e.g., "uscis", "ohss")
    """
    client = _get_weaviate()
    collection = client.collections.get("GovChunk")

    filters = None
    if agency:
        from weaviate.classes.query import Filter
        filters = Filter.by_property("agency").equal(agency)

    results = collection.query.bm25(
        query=query, limit=min(limit, 30), filters=filters
    )

    chunks = []
    for obj in results.objects:
        props = obj.properties
        chunks.append({
            "chunk_id": props.get("chunk_id", ""),
            "doc_id": props.get("doc_id", ""),
            "title": props.get("title", ""),
            "text": props.get("text", "")[:1000],
            "level": props.get("level", ""),
            "agency": props.get("agency", ""),
            "asset": props.get("asset", ""),
            "page_number": props.get("page_number"),
        })

    return json.dumps(chunks, indent=2)


@mcp.tool()
def get_weaviate_stats() -> str:
    """Get statistics about what's stored in the vector database.

    Returns collection names, object counts, and schema information.
    """
    client = _get_weaviate()
    collections = client.collections.list_all()

    stats = []
    for name, config in collections.items():
        collection = client.collections.get(name)
        count = collection.aggregate.over_all(total_count=True).total_count
        props = [{"name": p.name, "type": str(p.data_type)} for p in config.properties]
        stats.append({"collection": name, "object_count": count, "properties": props})

    return json.dumps(stats, indent=2)


# ---------------------------------------------------------------------------
# NEO4J TOOLS — Knowledge Graph
# ---------------------------------------------------------------------------

@mcp.tool()
def find_entity(name: str) -> str:
    """Find an entity in the knowledge graph and its connections.

    Returns the entity's type, aliases, documents that mention it,
    related entities, and time periods.

    Args:
        name: Entity name (e.g., "USCIS", "H-1B", "California", "I-130")
    """
    driver = _get_neo4j()
    with driver.session() as session:
        # Get entity details
        entity = session.run(
            "MATCH (e:Entity {canonical_name: $name}) RETURN e",
            {"name": name}
        ).single()

        if not entity:
            # Try case-insensitive search
            entity = session.run(
                "MATCH (e:Entity) WHERE toLower(e.canonical_name) = toLower($name) RETURN e",
                {"name": name}
            ).single()

        if not entity:
            return json.dumps({"error": f"Entity '{name}' not found"})

        e = entity["e"]

        # Get documents mentioning this entity
        docs = session.run(
            """MATCH (d:Document)-[r:MENTIONS]->(e:Entity {canonical_name: $name})
            RETURN d.asset AS asset, d.title AS title, d.agency AS agency, r.confidence AS confidence
            ORDER BY r.confidence DESC LIMIT 10""",
            {"name": e["canonical_name"]}
        )

        # Get related entities
        related = session.run(
            """MATCH (e:Entity {canonical_name: $name})-[r]-(other:Entity)
            RETURN other.canonical_name AS name, other.type AS type, type(r) AS relationship
            LIMIT 15""",
            {"name": e["canonical_name"]}
        )

        return json.dumps({
            "name": e["canonical_name"],
            "type": e.get("type", "unknown"),
            "aliases": e.get("aliases", []),
            "documents": [dict(d) for d in docs],
            "related_entities": [dict(r) for r in related],
        }, indent=2)


@mcp.tool()
def query_graph(cypher: str) -> str:
    """Run a read-only Cypher query against the Neo4j knowledge graph.

    The graph contains these node types:
    - Document (asset_key, doc_id, title, asset, agency, document_type, summary)
    - Entity (canonical_name, type: agency|geography|form|program|legislation)
    - Agency (name)
    - TimePeriod (period, start_date, end_date)

    And these relationships:
    - (Document)-[:MENTIONS]->(Entity) with confidence score
    - (Document)-[:PUBLISHED_BY]->(Agency)
    - (Document)-[:COVERS_PERIOD]->(TimePeriod)
    - (Entity)-[:RELATED_TO]->(Entity)
    - (Entity)-[:BELONGS_TO]->(Entity) for geographic hierarchy

    Args:
        cypher: A READ-ONLY Cypher query. Write operations are blocked.
    """
    # Security: block write operations
    cypher_upper = cypher.upper().strip()
    write_keywords = ["CREATE", "MERGE", "DELETE", "DETACH", "SET ", "REMOVE", "DROP"]
    for kw in write_keywords:
        if kw in cypher_upper:
            return json.dumps({"error": f"Write operations not allowed. Found '{kw}' in query."})

    driver = _get_neo4j()
    with driver.session() as session:
        result = session.run(cypher)
        records = [dict(r) for r in result]
        # Convert Neo4j types to JSON-serializable
        clean = []
        for record in records:
            clean_record = {}
            for k, v in record.items():
                if hasattr(v, "items"):  # Node
                    clean_record[k] = dict(v.items())
                elif isinstance(v, list):
                    clean_record[k] = [dict(x.items()) if hasattr(x, "items") else x for x in v]
                else:
                    clean_record[k] = v
            clean.append(clean_record)

        return json.dumps(clean[:100], indent=2, default=str)  # Cap at 100 rows


@mcp.tool()
def get_graph_overview() -> str:
    """Get a high-level overview of the knowledge graph structure.

    Returns node counts by type, relationship counts by type,
    and the top entities by mention count.
    """
    driver = _get_neo4j()
    with driver.session() as session:
        nodes = {}
        for label in ["Document", "Entity", "Agency", "TimePeriod"]:
            count = session.run(f"MATCH (n:{label}) RETURN count(n) AS c").single()["c"]
            nodes[label] = count

        rels = {}
        for rel_type in ["MENTIONS", "PUBLISHED_BY", "COVERS_PERIOD", "RELATED_TO", "BELONGS_TO"]:
            count = session.run(f"MATCH ()-[r:{rel_type}]->() RETURN count(r) AS c").single()["c"]
            rels[rel_type] = count

        top_entities = session.run(
            """MATCH (e:Entity)<-[r:MENTIONS]-()
            RETURN e.canonical_name AS name, e.type AS type, count(r) AS mentions
            ORDER BY mentions DESC LIMIT 10"""
        )

        return json.dumps({
            "node_counts": nodes,
            "relationship_counts": rels,
            "top_entities": [dict(e) for e in top_entities],
        }, indent=2)


@mcp.tool()
def find_path_between_entities(from_entity: str, to_entity: str, max_depth: int = 4) -> str:
    """Find the shortest path between two entities in the knowledge graph.

    Useful for discovering how two concepts are connected through
    documents, agencies, or other entities.

    Args:
        from_entity: Starting entity name
        to_entity: Target entity name
        max_depth: Maximum path length (default 4)
    """
    driver = _get_neo4j()
    with driver.session() as session:
        result = session.run(
            f"""MATCH (a {{canonical_name: $from}}), (b {{canonical_name: $to}}),
            path = shortestPath((a)-[*..{min(max_depth, 6)}]-(b))
            RETURN [n IN nodes(path) |
                CASE WHEN n.canonical_name IS NOT NULL THEN n.canonical_name
                     WHEN n.asset IS NOT NULL THEN n.asset
                     WHEN n.name IS NOT NULL THEN n.name
                     WHEN n.period IS NOT NULL THEN n.period
                     ELSE 'unknown' END
            ] AS node_names,
            [r IN relationships(path) | type(r)] AS rel_types""",
            {"from": from_entity, "to": to_entity}
        ).single()

        if not result:
            return json.dumps({"error": f"No path found between '{from_entity}' and '{to_entity}'"})

        return json.dumps({
            "nodes": result["node_names"],
            "relationships": result["rel_types"],
            "path_length": len(result["rel_types"]),
        }, indent=2)


# ---------------------------------------------------------------------------
# MINIO TOOLS — Document Storage
# ---------------------------------------------------------------------------

@mcp.tool()
def list_documents_in_storage(zone: str = "enrichment-zone", agency: str | None = None, limit: int = 50) -> str:
    """List documents stored in MinIO object storage.

    Documents are organized in three zones:
    - landing-zone: Raw downloaded files (PDF, XLSX, CSV)
    - parsed-zone: Structured parsed content (JSON)
    - enrichment-zone: LLM-enriched documents with entities, summaries, embeddings (JSON)

    Args:
        zone: Storage zone (landing-zone, parsed-zone, enrichment-zone)
        agency: Optional agency filter (e.g., "uscis")
        limit: Maximum results (default 50)
    """
    allowed_zones = ["landing-zone", "parsed-zone", "enrichment-zone"]
    if zone not in allowed_zones:
        return json.dumps({"error": f"Zone must be one of: {allowed_zones}"})

    client = _get_minio()
    bucket = os.getenv("MINIO_BUCKET", "gov-data-lake")
    prefix = f"{zone}/{agency}/" if agency else f"{zone}/"

    objects = list(client.list_objects(bucket, prefix=prefix, recursive=True))

    items = []
    for obj in objects[:limit]:
        items.append({
            "path": obj.object_name,
            "size_bytes": obj.size,
            "last_modified": obj.last_modified.isoformat() if obj.last_modified else None,
        })

    return json.dumps({"zone": zone, "count": len(items), "objects": items}, indent=2)


@mcp.tool()
def read_document(path: str) -> str:
    """Read a document from MinIO storage and return its content.

    For JSON files (parsed-zone, enrichment-zone): returns the parsed JSON content.
    For text files: returns the text content.
    For binary files (PDF, XLSX): returns metadata only (use list_documents_in_storage to find paths).

    Args:
        path: Full object path (e.g., "enrichment-zone/uscis/uscis-backlog-frontlog/2026-03-23/latest.json")
    """
    allowed_prefixes = ["landing-zone/", "parsed-zone/", "enrichment-zone/"]
    if not any(path.startswith(p) for p in allowed_prefixes):
        return json.dumps({"error": f"Path must start with one of: {allowed_prefixes}"})

    client = _get_minio()
    bucket = os.getenv("MINIO_BUCKET", "gov-data-lake")

    try:
        response = client.get_object(bucket, path)
        data = response.read()
        response.close()
    except Exception as e:
        return json.dumps({"error": f"Failed to read object: {e}"})

    # Try to parse as JSON
    if path.endswith(".json"):
        try:
            parsed = json.loads(data.decode("utf-8"))
            # Truncate large content fields to keep response manageable
            if isinstance(parsed, dict):
                content = parsed.get("content", {})
                if isinstance(content, dict):
                    sections = content.get("sections", [])
                    if len(sections) > 20:
                        content["sections"] = sections[:20]
                        content["_truncated"] = f"Showing 20 of {len(sections)} sections"
            return json.dumps(parsed, indent=2, default=str)
        except Exception:
            pass

    # For text files
    try:
        text = data.decode("utf-8")
        if len(text) > 10000:
            return text[:10000] + f"\n\n[TRUNCATED — showing 10,000 of {len(text)} characters]"
        return text
    except UnicodeDecodeError:
        return json.dumps({
            "path": path,
            "size_bytes": len(data),
            "type": "binary",
            "note": "Binary file — cannot display content. This is likely a PDF or XLSX file."
        })


# ---------------------------------------------------------------------------
# POSTGRESQL TOOLS — Pipeline Metadata
# ---------------------------------------------------------------------------

@mcp.tool()
def list_assets() -> str:
    """List all data assets (document sources) managed by the pipeline.

    Each asset represents a specific government data source (e.g., USCIS backlog report,
    Census population estimates). Returns asset names, descriptions, agencies, and configuration.
    """
    engine = _get_pg_engine()
    from sqlalchemy import text

    with engine.connect() as conn:
        result = conn.execute(text(
            """SELECT a.name, a.description, ag.name as agency_name,
                      a.created_at, a.updated_at
               FROM assets a
               LEFT JOIN agencies ag ON a.agency_id = ag.id
               ORDER BY ag.name, a.name"""
        ))
        assets = [dict(row._mapping) for row in result]

    return json.dumps(assets, indent=2, default=str)


@mcp.tool()
def list_agencies() -> str:
    """List all government agencies in the system with their asset counts.

    Returns agency names, full names, and the number of data assets from each.
    """
    engine = _get_pg_engine()
    from sqlalchemy import text

    with engine.connect() as conn:
        result = conn.execute(text(
            """SELECT ag.name, ag.full_name, ag.description, COUNT(a.id) as asset_count
               FROM agencies ag
               LEFT JOIN assets a ON a.agency_id = ag.id
               GROUP BY ag.id, ag.name, ag.full_name, ag.description
               ORDER BY asset_count DESC"""
        ))
        agencies = [dict(row._mapping) for row in result]

    return json.dumps(agencies, indent=2, default=str)


@mcp.tool()
def get_pipeline_stats() -> str:
    """Get overall pipeline statistics including workflow counts, quality metrics, and coverage.

    Returns aggregated metrics about the data ingestion pipeline: how many assets are ingested,
    success rates, quality scores, and processing efficiency.
    """
    engine = _get_pg_engine()
    from sqlalchemy import text

    with engine.connect() as conn:
        # Count assets and agencies
        asset_count = conn.execute(text("SELECT COUNT(*) FROM assets")).scalar()
        agency_count = conn.execute(text("SELECT COUNT(*) FROM agencies")).scalar()
        workflow_count = conn.execute(text("SELECT COUNT(*) FROM workflows")).scalar()

        return json.dumps({
            "total_assets": asset_count,
            "total_agencies": agency_count,
            "total_workflows": workflow_count,
        }, indent=2)


# ---------------------------------------------------------------------------
# WEB SEARCH TOOLS — Live .gov and USAFacts.org search via Firecrawl
# ---------------------------------------------------------------------------

_firecrawl_client = None


def _get_firecrawl():
    global _firecrawl_client
    if _firecrawl_client is None:
        api_key = os.getenv("FIRECRAWL_API_KEY")
        if not api_key:
            return None
        from firecrawl import FirecrawlApp
        _firecrawl_client = FirecrawlApp(api_key=api_key)
    return _firecrawl_client


def _firecrawl_search(query: str, site_filter: str, limit: int) -> list[dict]:
    """Run a Firecrawl search and parse the response."""
    client = _get_firecrawl()
    if not client:
        return []

    response = client.search(query=f"{query} {site_filter}", limit=limit)

    items = []
    if hasattr(response, "web") and response.web:
        items = response.web
    elif isinstance(response, list):
        items = response

    results = []
    for item in items[:limit]:
        if hasattr(item, "url"):
            url = item.url or ""
            title = item.title or ""
            description = item.description or ""
        else:
            url = item.get("url", "")
            title = item.get("title", "")
            description = item.get("description", "")

        results.append({
            "title": title,
            "url": url,
            "description": description[:1000],
        })

    return results


@mcp.tool()
def search_gov_websites(query: str, limit: int = 5) -> str:
    """Search .gov government websites for current information.

    Performs a live web search restricted to federal government domains (.gov).
    Use this for current facts, latest policies, recent statistics, or any
    information that may not be in the ingested document collection.

    Examples: current USCIS director, latest fee schedule, processing times,
    recent executive orders, current immigration policy.

    Args:
        query: Search query (e.g., "USCIS processing times I-130")
        limit: Maximum results (default 5, max 10)
    """
    try:
        results = _firecrawl_search(query, "site:*.gov", min(limit, 10))
        if not results:
            return json.dumps({"message": "No results found. FIRECRAWL_API_KEY may not be set or credits exhausted."})
        return json.dumps(results, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def search_usafacts(query: str, limit: int = 5) -> str:
    """Search USAFacts.org for curated US government data and statistics.

    USAFacts.org provides curated, contextualized government data with
    charts, analysis, and fact-checked statistics. Use this for:
    - Immigration statistics and trends
    - Government spending and revenue data
    - Population and demographic data
    - Crime statistics
    - Economic indicators
    - Any topic where you need curated, trustworthy US data

    Args:
        query: Search query (e.g., "immigration trends", "crime rate by state")
        limit: Maximum results (default 5, max 10)
    """
    try:
        results = _firecrawl_search(query, "site:usafacts.org", min(limit, 10))
        if not results:
            return json.dumps({"message": "No results found. FIRECRAWL_API_KEY may not be set or credits exhausted."})
        return json.dumps(results, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# RESOURCES — Context for Claude
# ---------------------------------------------------------------------------

@mcp.resource("data://schema/overview")
def schema_overview() -> str:
    """Overview of all data sources and their schemas."""
    return json.dumps({
        "weaviate": {
            "description": "Vector database with hybrid search (BM25 + semantic)",
            "collections": {
                "GovDocument": "37 government documents with metadata, summaries, topics, and 384-dim embeddings",
                "GovChunk": "2000+ text chunks (sections, tables) for granular retrieval",
            },
        },
        "neo4j": {
            "description": "Knowledge graph connecting documents, entities, agencies, and time periods",
            "nodes": ["Document", "Entity", "Agency", "TimePeriod"],
            "relationships": ["MENTIONS", "PUBLISHED_BY", "COVERS_PERIOD", "RELATED_TO", "BELONGS_TO"],
        },
        "minio": {
            "description": "Object storage for raw and processed documents",
            "zones": {
                "landing-zone": "Raw downloaded files (PDF, XLSX, CSV)",
                "parsed-zone": "Structured parsed content (JSON)",
                "enrichment-zone": "LLM-enriched documents with entities, summaries, embeddings",
            },
        },
        "postgresql": {
            "description": "Pipeline metadata, asset management, experiment tracking",
            "tables": ["agencies", "assets", "workflows", "experiments", "experiment_results"],
        },
    }, indent=2)


@mcp.resource("data://agencies")
def agencies_resource() -> str:
    """List of government agencies with their data assets."""
    try:
        engine = _get_pg_engine()
        from sqlalchemy import text
        with engine.connect() as conn:
            result = conn.execute(text(
                """SELECT ag.name, ag.full_name, COUNT(a.id) as assets
                   FROM agencies ag LEFT JOIN assets a ON a.agency_id = ag.id
                   GROUP BY ag.id, ag.name, ag.full_name ORDER BY assets DESC"""
            ))
            return json.dumps([dict(r._mapping) for r in result], indent=2, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    mcp.run()


if __name__ == "__main__":
    main()
