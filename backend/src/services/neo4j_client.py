"""Neo4j graph database client for entity relationships.

Manages a knowledge graph with nodes:
- Document: pipeline documents with metadata
- Entity: named entities (agencies, programs, forms, geographies)
- Agency: government agencies
- TimePeriod: fiscal quarters, years

And relationships:
- MENTIONS: Document -> Entity
- COVERS_PERIOD: Document -> TimePeriod
- PUBLISHED_BY: Document -> Agency
- RELATED_TO: Entity <-> Entity (co-occurrence)
- BELONGS_TO: Entity -> Entity (hierarchy, e.g. geography)
"""

import os
from logging import getLogger
from typing import Any

logger = getLogger(__name__)

_driver = None


def get_driver():
    """Get or create Neo4j driver."""
    global _driver
    if _driver is None:
        from neo4j import GraphDatabase

        uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        user = os.getenv("NEO4J_USER", "neo4j")
        password = os.getenv("NEO4J_PASSWORD", "pipeline123")
        _driver = GraphDatabase.driver(uri, auth=(user, password))
    return _driver


def ensure_constraints() -> None:
    """Create uniqueness constraints and indexes."""
    driver = get_driver()
    statements = [
        "CREATE CONSTRAINT doc_asset_key IF NOT EXISTS FOR (d:Document) REQUIRE d.asset_key IS UNIQUE",
        "CREATE CONSTRAINT agency_name IF NOT EXISTS FOR (a:Agency) REQUIRE a.name IS UNIQUE",
        "CREATE CONSTRAINT period_key IF NOT EXISTS FOR (t:TimePeriod) REQUIRE t.period IS UNIQUE",
        "CREATE INDEX entity_type_idx IF NOT EXISTS FOR (e:Entity) ON (e.type)",
        "CREATE INDEX entity_name_idx IF NOT EXISTS FOR (e:Entity) ON (e.canonical_name)",
        "CREATE INDEX doc_asset_idx IF NOT EXISTS FOR (d:Document) ON (d.asset)",
        "CREATE INDEX doc_id_idx IF NOT EXISTS FOR (d:Document) ON (d.doc_id)",
    ]
    with driver.session() as session:
        for cypher in statements:
            try:
                session.run(cypher)
            except Exception as e:
                logger.debug(f"Constraint/index setup: {e}")


def upsert_document(doc_data: dict[str, Any]) -> None:
    """MERGE a Document node using asset_key for stable deduplication."""
    driver = get_driver()
    agency = doc_data.get("agency", "")
    asset = doc_data.get("asset", "")
    asset_key = f"{agency}/{asset}" if agency and asset else doc_data.get("doc_id", "")
    with driver.session() as session:
        session.run(
            """
            MERGE (d:Document {asset_key: $asset_key})
            SET d.doc_id = $doc_id,
                d.title = $title,
                d.asset = $asset,
                d.agency = $agency,
                d.document_type = $document_type,
                d.storage_path = $storage_path,
                d.date = $date,
                d.summary = $summary,
                d.run_id = $run_id
            """,
            {**doc_data, "asset_key": asset_key},
        )


def upsert_entity(entity_data: dict[str, Any]) -> None:
    """MERGE an Entity node."""
    driver = get_driver()
    with driver.session() as session:
        session.run(
            """
            MERGE (e:Entity {canonical_name: $canonical_name, type: $type})
            SET e.aliases = $aliases,
                e.fips_code = $fips_code,
                e.iso_code = $iso_code,
                e.program_category = $program_category
            """,
            {
                "canonical_name": entity_data.get("canonical_name", ""),
                "type": entity_data.get("type", "other"),
                "aliases": entity_data.get("aliases", []),
                "fips_code": entity_data.get("fips_code", ""),
                "iso_code": entity_data.get("iso_code", ""),
                "program_category": entity_data.get("program_category", ""),
            },
        )


def upsert_agency(name: str, full_name: str = "", description: str = "") -> None:
    """MERGE an Agency node."""
    driver = get_driver()
    with driver.session() as session:
        session.run(
            """
            MERGE (a:Agency {name: $name})
            SET a.full_name = $full_name, a.description = $description
            """,
            {"name": name, "full_name": full_name, "description": description},
        )


def upsert_time_period(period: str, start_date: str = "", end_date: str = "") -> None:
    """MERGE a TimePeriod node."""
    driver = get_driver()
    with driver.session() as session:
        session.run(
            """
            MERGE (t:TimePeriod {period: $period})
            SET t.start_date = $start_date, t.end_date = $end_date
            """,
            {"period": period, "start_date": start_date, "end_date": end_date},
        )


def create_mentions(
    asset_key: str, canonical_name: str, entity_type: str, confidence: float = 1.0, context: str = ""
) -> None:
    """Create MENTIONS relationship between Document and Entity."""
    driver = get_driver()
    with driver.session() as session:
        session.run(
            """
            MATCH (d:Document {asset_key: $asset_key})
            MATCH (e:Entity {canonical_name: $canonical_name, type: $entity_type})
            MERGE (d)-[r:MENTIONS]->(e)
            SET r.confidence = $confidence, r.context = $context
            """,
            {
                "asset_key": asset_key,
                "canonical_name": canonical_name,
                "entity_type": entity_type,
                "confidence": confidence,
                "context": context,
            },
        )


def create_covers_period(asset_key: str, period: str) -> None:
    """Create COVERS_PERIOD relationship."""
    driver = get_driver()
    with driver.session() as session:
        session.run(
            """
            MATCH (d:Document {asset_key: $asset_key})
            MATCH (t:TimePeriod {period: $period})
            MERGE (d)-[:COVERS_PERIOD]->(t)
            """,
            {"asset_key": asset_key, "period": period},
        )


def create_published_by(asset_key: str, agency_name: str) -> None:
    """Create PUBLISHED_BY relationship."""
    driver = get_driver()
    with driver.session() as session:
        session.run(
            """
            MATCH (d:Document {asset_key: $asset_key})
            MATCH (a:Agency {name: $agency_name})
            MERGE (d)-[:PUBLISHED_BY]->(a)
            """,
            {"asset_key": asset_key, "agency_name": agency_name},
        )


def create_entity_relationship(
    from_name: str, from_type: str, to_name: str, to_type: str, rel_type: str = "RELATED_TO"
) -> None:
    """Create relationship between two entities."""
    driver = get_driver()
    with driver.session() as session:
        session.run(
            f"""
            MATCH (a:Entity {{canonical_name: $from_name, type: $from_type}})
            MATCH (b:Entity {{canonical_name: $to_name, type: $to_type}})
            MERGE (a)-[:{rel_type}]->(b)
            """,
            {"from_name": from_name, "from_type": from_type, "to_name": to_name, "to_type": to_type},
        )


def find_related_entities(entity_name: str, depth: int = 2, limit: int = 20) -> list[dict[str, Any]]:
    """Find entities related to a given entity via graph traversal."""
    driver = get_driver()
    with driver.session() as session:
        result = session.run(
            """
            MATCH (e:Entity {canonical_name: $name})-[*1..2]-(related:Entity)
            WHERE related <> e
            RETURN DISTINCT related.canonical_name AS name,
                   related.type AS type
            LIMIT $limit
            """,
            {"name": entity_name, "limit": limit},
        )
        return [dict(record) for record in result]


def find_documents_by_entity(entity_name: str, limit: int = 20) -> list[dict[str, Any]]:
    """Find documents mentioning a specific entity."""
    driver = get_driver()
    with driver.session() as session:
        result = session.run(
            """
            MATCH (d:Document)-[r:MENTIONS]->(e:Entity {canonical_name: $name})
            RETURN d.doc_id AS doc_id, d.title AS title, d.asset AS asset,
                   d.agency AS agency, r.confidence AS confidence
            ORDER BY r.confidence DESC
            LIMIT $limit
            """,
            {"name": entity_name, "limit": limit},
        )
        return [dict(record) for record in result]


def find_document_context(doc_id: str) -> dict[str, Any]:
    """Get the graph neighborhood of a document."""
    driver = get_driver()
    with driver.session() as session:
        entities = [
            dict(r)
            for r in session.run(
                """
                MATCH (d:Document {doc_id: $doc_id})-[r:MENTIONS]->(e:Entity)
                RETURN e.canonical_name AS name, e.type AS type, r.confidence AS confidence
                ORDER BY r.confidence DESC
                """,
                {"doc_id": doc_id},
            )
        ]

        related_docs = [
            dict(r)
            for r in session.run(
                """
                MATCH (d:Document {doc_id: $doc_id})-[:MENTIONS]->(e:Entity)<-[:MENTIONS]-(other:Document)
                WHERE other.doc_id <> $doc_id
                WITH other, count(e) AS shared_entities
                RETURN other.doc_id AS doc_id, other.title AS title,
                       other.asset AS asset, shared_entities
                ORDER BY shared_entities DESC
                LIMIT 5
                """,
                {"doc_id": doc_id},
            )
        ]

        periods = [
            dict(r)
            for r in session.run(
                """
                MATCH (d:Document {doc_id: $doc_id})-[:COVERS_PERIOD]->(t:TimePeriod)
                RETURN t.period AS period, t.start_date AS start_date, t.end_date AS end_date
                """,
                {"doc_id": doc_id},
            )
        ]

        agency_rows = [
            dict(r)
            for r in session.run(
                """
                MATCH (d:Document {doc_id: $doc_id})-[:PUBLISHED_BY]->(a:Agency)
                RETURN a.name AS name, a.full_name AS full_name
                """,
                {"doc_id": doc_id},
            )
        ]

        return {
            "doc_id": doc_id,
            "entities": entities,
            "related_documents": related_docs,
            "time_periods": periods,
            "agency": agency_rows[0] if agency_rows else None,
        }


def close() -> None:
    """Close Neo4j driver."""
    global _driver
    if _driver is not None:
        _driver.close()
        _driver = None


_entity_cache = None
_entity_cache_time = 0


def get_all_entity_names(limit: int = 500) -> list[dict[str, str]]:
    """Return all entity canonical names and types. Cached for 10 minutes."""
    global _entity_cache, _entity_cache_time
    import time
    now = time.time()
    if _entity_cache is not None and (now - _entity_cache_time) < 600:
        return _entity_cache
    try:
        driver = get_driver()
        with driver.session() as session:
            result = session.run(
                "MATCH (e:Entity) RETURN e.canonical_name AS name, e.type AS type ORDER BY e.canonical_name LIMIT $limit",
                {"limit": limit},
            )
            _entity_cache = [dict(r) for r in result]
            _entity_cache_time = now
            return _entity_cache
    except Exception:
        return _entity_cache or []
