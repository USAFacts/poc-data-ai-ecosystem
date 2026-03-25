"""Graph builder — extracts nodes and edges from enriched documents for Neo4j."""

import json
from logging import getLogger
from typing import Any

from src.services import neo4j_client

logger = getLogger(__name__)


def build_from_enriched_document(enriched_doc: dict[str, Any]) -> dict[str, int]:
    """Extract all graph nodes and relationships from one enriched document.

    Args:
        enriched_doc: Full enriched document from enrichment-zone.

    Returns:
        Counts of created nodes and relationships.
    """
    metadata = enriched_doc.get("metadata", {})
    source = enriched_doc.get("source", {})
    enrichment = enriched_doc.get("enrichment", {})
    doc_enrichment = enrichment.get("document", {})

    doc_id = metadata.get("identifier", "")
    if not doc_id:
        return {"nodes": 0, "relationships": 0}

    # Stable key for deduplication: agency/asset
    agency_name = source.get("agency", "")
    asset_name = source.get("asset", "")
    asset_key = f"{agency_name}/{asset_name}" if agency_name and asset_name else doc_id

    node_count = 0
    rel_count = 0

    # 1. Document node
    neo4j_client.upsert_document(
        {
            "doc_id": doc_id,
            "title": metadata.get("title", ""),
            "asset": asset_name,
            "agency": agency_name,
            "document_type": doc_enrichment.get("documentType", ""),
            "storage_path": source.get("enrichedStorageUrl", ""),
            "date": metadata.get("date", ""),
            "summary": doc_enrichment.get("summary", ""),
            "run_id": source.get("run_id", ""),
        }
    )
    node_count += 1

    # 2. Agency node + PUBLISHED_BY
    if agency_name:
        neo4j_client.upsert_agency(agency_name)
        neo4j_client.create_published_by(asset_key, agency_name)
        node_count += 1
        rel_count += 1

    # 3. Entity nodes + MENTIONS
    entities = doc_enrichment.get("entities", [])
    entity_keys: list[tuple[str, str]] = []

    for entity in entities:
        canonical = entity.get("canonicalName") or entity.get("name", "")
        etype = entity.get("type", "other")
        if not canonical:
            continue

        neo4j_client.upsert_entity(
            {
                "canonical_name": canonical,
                "type": etype,
                "aliases": entity.get("aliases", []),
                "fips_code": entity.get("fipsCode", ""),
                "iso_code": entity.get("isoCode", ""),
                "program_category": entity.get("programCategory", ""),
            }
        )
        node_count += 1

        neo4j_client.create_mentions(
            asset_key=asset_key,
            canonical_name=canonical,
            entity_type=etype,
            confidence=entity.get("confidence", 1.0),
            context=entity.get("context", "")[:500],
        )
        rel_count += 1
        entity_keys.append((canonical, etype))

        # Geography hierarchy
        parent = entity.get("parentGeography")
        if parent and etype == "geography":
            neo4j_client.upsert_entity(
                {
                    "canonical_name": parent,
                    "type": "geography",
                    "aliases": [],
                    "fips_code": "",
                    "iso_code": "",
                    "program_category": "",
                }
            )
            neo4j_client.create_entity_relationship(canonical, "geography", parent, "geography", "BELONGS_TO")
            node_count += 1
            rel_count += 1

    # 4. TimePeriod node + COVERS_PERIOD
    temporal = doc_enrichment.get("temporalScope", {})
    if temporal and temporal.get("period"):
        neo4j_client.upsert_time_period(
            period=temporal["period"],
            start_date=temporal.get("startDate", ""),
            end_date=temporal.get("endDate", ""),
        )
        neo4j_client.create_covers_period(asset_key, temporal["period"])
        node_count += 1
        rel_count += 1

    # 5. Co-occurrence RELATED_TO between entities of different types
    for i, (name_a, type_a) in enumerate(entity_keys):
        for name_b, type_b in entity_keys[i + 1 :]:
            if type_a != type_b:
                neo4j_client.create_entity_relationship(name_a, type_a, name_b, type_b, "RELATED_TO")
                rel_count += 1

    return {"nodes": node_count, "relationships": rel_count}


def sync_from_storage(storage) -> dict[str, int]:
    """Sync all enriched documents from MinIO to Neo4j."""
    total_nodes = 0
    total_rels = 0
    doc_count = 0

    try:
        enrichment_prefix = "enrichment-zone/"
        objects = list(storage.client.list_objects(storage.bucket, prefix=enrichment_prefix, recursive=True))

        asset_latest: dict[str, Any] = {}
        for obj in objects:
            if not obj.object_name.endswith(".json") or obj.object_name.endswith("_metadata.json"):
                continue
            parts = obj.object_name.split("/")
            if len(parts) >= 5:
                asset_key = f"{parts[1]}/{parts[2]}"
                if asset_key not in asset_latest or obj.last_modified > asset_latest[asset_key].last_modified:
                    asset_latest[asset_key] = obj

        for asset_key, obj in asset_latest.items():
            try:
                data = storage.get_object(obj.object_name)
                enriched_doc = json.loads(data.decode("utf-8"))
                counts = build_from_enriched_document(enriched_doc)
                total_nodes += counts["nodes"]
                total_rels += counts["relationships"]
                doc_count += 1
            except Exception as e:
                logger.warning(f"Failed to build graph for {obj.object_name}: {e}")

    except Exception as e:
        logger.error(f"Failed to sync to Neo4j: {e}")

    logger.info(f"Neo4j sync complete: {doc_count} documents, {total_nodes} nodes, {total_rels} relationships")
    return {"documents": doc_count, "nodes": total_nodes, "relationships": total_rels}
