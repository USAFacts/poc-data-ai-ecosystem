"""Neo4j status API routes for detailed graph database information."""

import os
from logging import getLogger
from typing import Any

from fastapi import APIRouter, HTTPException

logger = getLogger(__name__)

router = APIRouter()


@router.get("/status", summary="Neo4j Detailed Status")
def neo4j_status() -> dict[str, Any]:
    """Return detailed Neo4j status including node/relationship counts, top entities, and schema."""
    try:
        from src.services.neo4j_client import get_driver

        driver = get_driver()
        uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")

        with driver.session() as session:
            # Node counts by label
            node_counts = {}
            for label in ["Document", "Entity", "Agency", "TimePeriod"]:
                result = session.run(f"MATCH (n:`{label}`) RETURN count(n) AS count")
                record = result.single()
                node_counts[label] = record["count"] if record else 0

            # Relationship counts by type
            rel_counts = {}
            for rel_type in ["MENTIONS", "PUBLISHED_BY", "COVERS_PERIOD", "RELATED_TO", "BELONGS_TO"]:
                result = session.run(f"MATCH ()-[r:`{rel_type}`]->() RETURN count(r) AS count")
                record = result.single()
                rel_counts[rel_type] = record["count"] if record else 0

            # Top entities by mention count
            top_entities_result = session.run(
                "MATCH (e:Entity)<-[r:MENTIONS]-() "
                "RETURN e.canonical_name AS name, e.type AS type, count(r) AS mention_count "
                "ORDER BY mention_count DESC LIMIT 10"
            )
            top_entities = [
                {"name": r["name"], "type": r["type"], "mention_count": r["mention_count"]}
                for r in top_entities_result
            ]

            # Top agencies by document count
            top_agencies_result = session.run(
                "MATCH (a:Agency)<-[r:PUBLISHED_BY]-() "
                "RETURN a.name AS name, count(r) AS document_count "
                "ORDER BY document_count DESC LIMIT 10"
            )
            top_agencies = [
                {"name": r["name"], "document_count": r["document_count"]}
                for r in top_agencies_result
            ]

            # Constraints
            constraints_result = session.run("SHOW CONSTRAINTS")
            constraints = [
                {"name": r.get("name", ""), "type": r.get("type", ""), "entityType": r.get("entityType", "")}
                for r in constraints_result
            ]

            # Indexes
            indexes_result = session.run("SHOW INDEXES")
            indexes = [
                {"name": r.get("name", ""), "type": r.get("type", ""), "entityType": r.get("entityType", ""),
                 "state": r.get("state", "")}
                for r in indexes_result
            ]

            # Graph schema overview
            schema_result = session.run(
                "MATCH (a)-[r]->(b) "
                "RETURN DISTINCT labels(a)[0] AS from_label, type(r) AS rel_type, labels(b)[0] AS to_label"
            )
            schema_overview = [
                {"from": r["from_label"], "relationship": r["rel_type"], "to": r["to_label"]}
                for r in schema_result
            ]

        return {
            "status": "connected",
            "connection": {"uri": uri},
            "node_counts": node_counts,
            "relationship_counts": rel_counts,
            "top_entities": top_entities,
            "top_agencies": top_agencies,
            "constraints": constraints,
            "indexes": indexes,
            "schema_overview": schema_overview,
        }

    except Exception as e:
        logger.error(f"Neo4j status check failed: {e}")
        raise HTTPException(status_code=503, detail=f"Neo4j unavailable: {str(e)}")


# Color mapping for node types in visualization
NODE_COLORS = {
    "Document": "#4A90D9",
    "Entity": "#E8A838",
    "Agency": "#50C878",
    "TimePeriod": "#DA70D6",
}


@router.get("/schema", summary="Neo4j Graph Schema for Visualization")
def neo4j_schema() -> dict[str, Any]:
    """Return a simplified graph schema suitable for frontend visualization."""
    try:
        from src.services.neo4j_client import get_driver

        driver = get_driver()

        with driver.session() as session:
            # Nodes with counts and colors
            nodes = []
            for label in ["Document", "Entity", "Agency", "TimePeriod"]:
                result = session.run(f"MATCH (n:`{label}`) RETURN count(n) AS count")
                record = result.single()
                count = record["count"] if record else 0
                nodes.append({
                    "label": label,
                    "count": count,
                    "color": NODE_COLORS.get(label, "#999999"),
                })

            # Edges with counts
            edges_result = session.run(
                "MATCH (a)-[r]->(b) "
                "RETURN labels(a)[0] AS from_label, labels(b)[0] AS to_label, "
                "type(r) AS rel_type, count(r) AS count "
                "ORDER BY count DESC"
            )
            edges = [
                {
                    "from": r["from_label"],
                    "to": r["to_label"],
                    "type": r["rel_type"],
                    "count": r["count"],
                }
                for r in edges_result
            ]

        return {
            "nodes": nodes,
            "edges": edges,
        }

    except Exception as e:
        logger.error(f"Neo4j schema retrieval failed: {e}")
        raise HTTPException(status_code=503, detail=f"Neo4j unavailable: {str(e)}")
