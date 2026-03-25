"""Weaviate and Neo4j client wrappers for the sync step.

These are lightweight clients used by the ingestion pipeline to push
data directly to the search and graph backends after enrichment.
They mirror the backend service logic but are independent so the
pipeline doesn't depend on the FastAPI backend.
"""

import atexit
import json
import os
import threading
import uuid
from typing import Any

from logging_manager import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Weaviate
# ---------------------------------------------------------------------------

_weaviate_client = None
_weaviate_lock = threading.Lock()

GOV_DOCUMENT_COLLECTION = "GovDocument"
GOV_CHUNK_COLLECTION = "GovChunk"


def _get_weaviate_client():
    global _weaviate_client
    if _weaviate_client is not None:
        return _weaviate_client

    with _weaviate_lock:
        if _weaviate_client is not None:
            return _weaviate_client

        import weaviate

        host = os.getenv("WEAVIATE_HOST", "localhost")
        port = int(os.getenv("WEAVIATE_PORT", "8080"))
        grpc_port = int(os.getenv("WEAVIATE_GRPC_PORT", "50051"))
        _weaviate_client = weaviate.connect_to_local(host=host, port=port, grpc_port=grpc_port)
        atexit.register(_close_weaviate)
    return _weaviate_client


def _close_weaviate():
    global _weaviate_client
    if _weaviate_client is not None:
        try:
            _weaviate_client.close()
        except Exception:
            pass
        _weaviate_client = None


def sync_to_weaviate(enriched_doc: dict[str, Any], chunks: list[dict[str, Any]] | None = None) -> None:
    """Index an enriched document and its chunks into Weaviate.

    Uses deterministic UUIDs so re-runs upsert (delete + insert).
    Deletes all existing chunks for the document first to avoid orphans.
    """
    client = _get_weaviate_client()

    metadata = enriched_doc.get("metadata", {})
    source = enriched_doc.get("source", {})
    enrichment = enriched_doc.get("enrichment", {})
    doc_enrichment = enrichment.get("document", {})

    doc_id = metadata.get("identifier", "")
    if not doc_id:
        raise ValueError("Enriched document missing metadata.identifier")

    # Stable UUID key: agency/asset — so re-runs overwrite previous versions
    # instead of creating duplicates (doc_id includes a timestamp).
    agency = source.get("agency", "")
    asset = source.get("asset", "")
    stable_key = f"{agency}/{asset}" if agency and asset else doc_id

    entities = doc_enrichment.get("entities", [])
    entity_names = [e.get("canonicalName") or e.get("name", "") for e in entities]
    temporal = doc_enrichment.get("temporalScope", {})
    embedding = enrichment.get("embedding", {}).get("vector")

    # Upsert document
    doc_collection = client.collections.get(GOV_DOCUMENT_COLLECTION)
    doc_uuid = uuid.uuid5(uuid.NAMESPACE_URL, stable_key)

    doc_props = {
        "doc_id": doc_id,
        "title": metadata.get("title", ""),
        "summary": doc_enrichment.get("summary", ""),
        "agency": source.get("agency", ""),
        "asset": source.get("asset", ""),
        "document_type": doc_enrichment.get("documentType", ""),
        "key_topics": doc_enrichment.get("keyTopics", []),
        "date_str": metadata.get("date", ""),
        "temporal_period": temporal.get("period", "") if temporal else "",
        "original_url": source.get("originalUrl", ""),
        "storage_path": source.get("enrichedStorageUrl", ""),
        "entity_names": entity_names,
        "run_id": source.get("run_id", ""),
    }

    try:
        doc_collection.data.delete_by_id(doc_uuid)
    except Exception:
        pass

    if embedding:
        doc_collection.data.insert(properties=doc_props, vector=embedding, uuid=doc_uuid)
    else:
        doc_collection.data.insert(properties=doc_props, uuid=doc_uuid)

    # Delete all existing chunks for this asset before inserting new ones
    chunk_collection = client.collections.get(GOV_CHUNK_COLLECTION)
    try:
        from weaviate.classes.query import Filter

        old_chunks = chunk_collection.query.fetch_objects(
            filters=Filter.by_property("asset").equal(asset),
            limit=10000,
        )
        for old in old_chunks.objects:
            chunk_collection.data.delete_by_id(old.uuid)
    except Exception:
        pass

    # Insert new chunks
    if chunks:
        from services.embeddings import get_embedding

        for chunk in chunks:
            chunk_text = chunk.get("text", "")
            chunk_meta = chunk.get("metadata", {})

            chunk_props = {
                "chunk_id": chunk.get("chunk_id", ""),
                "parent_chunk_id": chunk.get("parent_chunk_id", ""),
                "doc_id": doc_id,
                "level": chunk.get("level", ""),
                "sequence": chunk.get("sequence", 0),
                "title": chunk_meta.get("title", ""),
                "text": chunk_text,
                "agency": source.get("agency", ""),
                "asset": source.get("asset", ""),
                "page_number": chunk_meta.get("page_number") or 0,
                "section_id": chunk_meta.get("section_id", ""),
                "table_id": chunk_meta.get("table_id", ""),
                "run_id": source.get("run_id", ""),
            }

            chunk_embedding = get_embedding(chunk_text) if chunk_text else None

            # Stable chunk UUID: asset + level + sequence so re-runs overwrite
            chunk_stable_key = f"{stable_key}/{chunk.get('level', '')}/{chunk.get('sequence', 0)}"
            chunk_uuid = uuid.uuid5(uuid.NAMESPACE_URL, chunk_stable_key)

            if chunk_embedding:
                chunk_collection.data.insert(properties=chunk_props, vector=chunk_embedding, uuid=chunk_uuid)
            else:
                chunk_collection.data.insert(properties=chunk_props, uuid=chunk_uuid)


# ---------------------------------------------------------------------------
# Neo4j
# ---------------------------------------------------------------------------

_neo4j_driver = None
_neo4j_lock = threading.Lock()


def _get_neo4j_driver():
    global _neo4j_driver
    if _neo4j_driver is not None:
        return _neo4j_driver

    with _neo4j_lock:
        if _neo4j_driver is not None:
            return _neo4j_driver

        from neo4j import GraphDatabase

        uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        user = os.getenv("NEO4J_USER", "neo4j")
        password = os.getenv("NEO4J_PASSWORD", "pipeline123")
        _neo4j_driver = GraphDatabase.driver(uri, auth=(user, password))
        atexit.register(_close_neo4j)
    return _neo4j_driver


def _close_neo4j():
    global _neo4j_driver
    if _neo4j_driver is not None:
        try:
            _neo4j_driver.close()
        except Exception:
            pass
        _neo4j_driver = None


def sync_to_neo4j(enriched_doc: dict[str, Any]) -> None:
    """Build graph nodes and relationships from an enriched document.

    Uses MERGE (upsert) for all operations — safe to call repeatedly.
    """
    driver = _get_neo4j_driver()

    metadata = enriched_doc.get("metadata", {})
    source = enriched_doc.get("source", {})
    enrichment = enriched_doc.get("enrichment", {})
    doc_enrichment = enrichment.get("document", {})

    doc_id = metadata.get("identifier", "")
    if not doc_id:
        raise ValueError("Enriched document missing metadata.identifier")

    # Use asset as the stable merge key so re-runs update the same node
    agency = source.get("agency", "")
    asset = source.get("asset", "")
    stable_key = f"{agency}/{asset}" if agency and asset else doc_id

    with driver.session() as session:
        # Document node — merge on stable asset key, update all properties
        session.run(
            """
            MERGE (d:Document {asset_key: $asset_key})
            SET d.doc_id = $doc_id, d.title = $title,
                d.asset = $asset, d.agency = $agency,
                d.document_type = $document_type, d.date = $date,
                d.summary = $summary, d.run_id = $run_id
            """,
            {
                "asset_key": stable_key,
                "doc_id": doc_id,
                "title": metadata.get("title", ""),
                "asset": asset,
                "agency": agency,
                "document_type": doc_enrichment.get("documentType", ""),
                "date": metadata.get("date", ""),
                "summary": doc_enrichment.get("summary", ""),
                "run_id": source.get("run_id", ""),
            },
        )

        # Agency node + PUBLISHED_BY
        if agency:
            session.run(
                "MERGE (a:Agency {name: $name})",
                {"name": agency},
            )
            session.run(
                """
                MATCH (d:Document {asset_key: $asset_key})
                MATCH (a:Agency {name: $agency})
                MERGE (d)-[:PUBLISHED_BY]->(a)
                """,
                {"asset_key": stable_key, "agency": agency},
            )

        # Entity nodes + MENTIONS
        entities = doc_enrichment.get("entities", [])
        entity_keys: list[tuple[str, str]] = []

        for entity in entities:
            canonical = entity.get("canonicalName") or entity.get("name", "")
            etype = entity.get("type", "other")
            if not canonical:
                continue

            session.run(
                """
                MERGE (e:Entity {canonical_name: $canonical_name, type: $type})
                SET e.aliases = $aliases
                """,
                {
                    "canonical_name": canonical,
                    "type": etype,
                    "aliases": entity.get("aliases", []),
                },
            )

            session.run(
                """
                MATCH (d:Document {asset_key: $asset_key})
                MATCH (e:Entity {canonical_name: $name, type: $type})
                MERGE (d)-[r:MENTIONS]->(e)
                SET r.confidence = $confidence
                """,
                {
                    "asset_key": stable_key,
                    "name": canonical,
                    "type": etype,
                    "confidence": entity.get("confidence", 1.0),
                },
            )
            entity_keys.append((canonical, etype))

            # Geography hierarchy
            parent = entity.get("parentGeography")
            if parent and etype == "geography":
                session.run(
                    "MERGE (e:Entity {canonical_name: $name, type: 'geography'})",
                    {"name": parent},
                )
                session.run(
                    """
                    MATCH (a:Entity {canonical_name: $child, type: 'geography'})
                    MATCH (b:Entity {canonical_name: $parent, type: 'geography'})
                    MERGE (a)-[:BELONGS_TO]->(b)
                    """,
                    {"child": canonical, "parent": parent},
                )

        # TimePeriod node + COVERS_PERIOD
        temporal = doc_enrichment.get("temporalScope", {})
        if temporal and temporal.get("period"):
            session.run(
                """
                MERGE (t:TimePeriod {period: $period})
                SET t.start_date = $start_date, t.end_date = $end_date
                """,
                {
                    "period": temporal["period"],
                    "start_date": temporal.get("startDate", ""),
                    "end_date": temporal.get("endDate", ""),
                },
            )
            session.run(
                """
                MATCH (d:Document {asset_key: $asset_key})
                MATCH (t:TimePeriod {period: $period})
                MERGE (d)-[:COVERS_PERIOD]->(t)
                """,
                {"asset_key": stable_key, "period": temporal["period"]},
            )

        # Co-occurrence RELATED_TO between entities of different types
        for i, (name_a, type_a) in enumerate(entity_keys):
            for name_b, type_b in entity_keys[i + 1:]:
                if type_a != type_b:
                    session.run(
                        """
                        MATCH (a:Entity {canonical_name: $from_name, type: $from_type})
                        MATCH (b:Entity {canonical_name: $to_name, type: $to_type})
                        MERGE (a)-[:RELATED_TO]->(b)
                        """,
                        {
                            "from_name": name_a,
                            "from_type": type_a,
                            "to_name": name_b,
                            "to_type": type_b,
                        },
                    )
