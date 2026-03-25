"""Weaviate client for hybrid search (BM25 + vector).

Manages two collections:
- GovDocument: document-level metadata + embedding
- GovChunk: per-chunk text + embedding for granular retrieval

Uses pre-computed embeddings (no Weaviate vectorizer modules).
"""

import json
import os
import uuid
from logging import getLogger
from typing import Any

logger = getLogger(__name__)

_client = None

GOV_DOCUMENT_COLLECTION = "GovDocument"
GOV_CHUNK_COLLECTION = "GovChunk"


def get_client():
    """Get or create Weaviate client (v4 API)."""
    global _client
    if _client is None:
        import weaviate

        host = os.getenv("WEAVIATE_HOST", "localhost")
        port = int(os.getenv("WEAVIATE_PORT", "8080"))
        grpc_port = int(os.getenv("WEAVIATE_GRPC_PORT", "50051"))
        _client = weaviate.connect_to_local(host=host, port=port, grpc_port=grpc_port)
    return _client


def ensure_schema() -> None:
    """Create GovDocument and GovChunk collections if they don't exist."""
    import weaviate.classes.config as wc

    client = get_client()

    if not client.collections.exists(GOV_DOCUMENT_COLLECTION):
        client.collections.create(
            name=GOV_DOCUMENT_COLLECTION,
            vectorizer_config=wc.Configure.Vectorizer.none(),
            properties=[
                wc.Property(name="doc_id", data_type=wc.DataType.TEXT, tokenization=wc.Tokenization.FIELD),
                wc.Property(name="title", data_type=wc.DataType.TEXT, tokenization=wc.Tokenization.WORD),
                wc.Property(name="summary", data_type=wc.DataType.TEXT, tokenization=wc.Tokenization.WORD),
                wc.Property(name="agency", data_type=wc.DataType.TEXT, tokenization=wc.Tokenization.FIELD),
                wc.Property(name="asset", data_type=wc.DataType.TEXT, tokenization=wc.Tokenization.FIELD),
                wc.Property(name="document_type", data_type=wc.DataType.TEXT, tokenization=wc.Tokenization.FIELD),
                wc.Property(name="key_topics", data_type=wc.DataType.TEXT_ARRAY),
                wc.Property(name="date_str", data_type=wc.DataType.TEXT, tokenization=wc.Tokenization.FIELD),
                wc.Property(name="temporal_period", data_type=wc.DataType.TEXT, tokenization=wc.Tokenization.FIELD),
                wc.Property(name="original_url", data_type=wc.DataType.TEXT, tokenization=wc.Tokenization.FIELD),
                wc.Property(name="storage_path", data_type=wc.DataType.TEXT, tokenization=wc.Tokenization.FIELD),
                wc.Property(name="entity_names", data_type=wc.DataType.TEXT_ARRAY),
                wc.Property(name="run_id", data_type=wc.DataType.TEXT, tokenization=wc.Tokenization.FIELD),
            ],
        )
        logger.info("Created GovDocument collection")

    if not client.collections.exists(GOV_CHUNK_COLLECTION):
        client.collections.create(
            name=GOV_CHUNK_COLLECTION,
            vectorizer_config=wc.Configure.Vectorizer.none(),
            properties=[
                wc.Property(name="chunk_id", data_type=wc.DataType.TEXT, tokenization=wc.Tokenization.FIELD),
                wc.Property(name="parent_chunk_id", data_type=wc.DataType.TEXT, tokenization=wc.Tokenization.FIELD),
                wc.Property(name="doc_id", data_type=wc.DataType.TEXT, tokenization=wc.Tokenization.FIELD),
                wc.Property(name="level", data_type=wc.DataType.TEXT, tokenization=wc.Tokenization.FIELD),
                wc.Property(name="sequence", data_type=wc.DataType.INT),
                wc.Property(name="title", data_type=wc.DataType.TEXT, tokenization=wc.Tokenization.WORD),
                wc.Property(name="text", data_type=wc.DataType.TEXT, tokenization=wc.Tokenization.WORD),
                wc.Property(name="agency", data_type=wc.DataType.TEXT, tokenization=wc.Tokenization.FIELD),
                wc.Property(name="asset", data_type=wc.DataType.TEXT, tokenization=wc.Tokenization.FIELD),
                wc.Property(name="page_number", data_type=wc.DataType.INT),
                wc.Property(name="section_id", data_type=wc.DataType.TEXT, tokenization=wc.Tokenization.FIELD),
                wc.Property(name="table_id", data_type=wc.DataType.TEXT, tokenization=wc.Tokenization.FIELD),
                wc.Property(name="run_id", data_type=wc.DataType.TEXT, tokenization=wc.Tokenization.FIELD),
            ],
        )
        logger.info("Created GovChunk collection")


def index_document(
    enriched_doc: dict[str, Any],
    chunks: list[dict[str, Any]] | None = None,
    embedding: list[float] | None = None,
) -> None:
    """Index an enriched document and its chunks into Weaviate."""
    client = get_client()

    metadata = enriched_doc.get("metadata", {})
    source = enriched_doc.get("source", {})
    enrichment = enriched_doc.get("enrichment", {})
    doc_enrichment = enrichment.get("document", {})

    doc_id = metadata.get("identifier", "")
    agency = source.get("agency", "")
    asset = source.get("asset", "")
    stable_key = f"{agency}/{asset}" if agency and asset else doc_id

    entities = doc_enrichment.get("entities", [])
    entity_names = [e.get("canonicalName") or e.get("name", "") for e in entities]
    temporal = doc_enrichment.get("temporalScope", {})

    if embedding is None:
        emb_data = enrichment.get("embedding", {})
        embedding = emb_data.get("vector")

    doc_collection = client.collections.get(GOV_DOCUMENT_COLLECTION)

    doc_props = {
        "doc_id": doc_id,
        "title": metadata.get("title", ""),
        "summary": doc_enrichment.get("summary", ""),
        "agency": agency,
        "asset": asset,
        "document_type": doc_enrichment.get("documentType", ""),
        "key_topics": doc_enrichment.get("keyTopics", []),
        "date_str": metadata.get("date", ""),
        "temporal_period": temporal.get("period", "") if temporal else "",
        "original_url": source.get("originalUrl", ""),
        "storage_path": source.get("enrichedStorageUrl", ""),
        "entity_names": entity_names,
        "run_id": source.get("run_id", ""),
    }

    doc_uuid = uuid.uuid5(uuid.NAMESPACE_URL, stable_key)

    try:
        doc_collection.data.delete_by_id(doc_uuid)
    except Exception:
        pass

    if embedding is not None:
        doc_collection.data.insert(properties=doc_props, vector=embedding, uuid=doc_uuid)
    else:
        doc_collection.data.insert(properties=doc_props, uuid=doc_uuid)

    # Index chunks
    if chunks:
        chunk_collection = client.collections.get(GOV_CHUNK_COLLECTION)

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

            # Compute chunk embedding
            chunk_embedding = None
            try:
                from src.services.embeddings import get_embedding
                chunk_embedding = get_embedding(chunk_text) if chunk_text else None
            except Exception:
                pass

            chunk_stable_key = f"{stable_key}/{chunk.get('level', '')}/{chunk.get('sequence', 0)}"
            chunk_uuid = uuid.uuid5(uuid.NAMESPACE_URL, chunk_stable_key)

            try:
                chunk_collection.data.delete_by_id(chunk_uuid)
            except Exception:
                pass

            if chunk_embedding is not None:
                chunk_collection.data.insert(properties=chunk_props, vector=chunk_embedding, uuid=chunk_uuid)
            else:
                chunk_collection.data.insert(properties=chunk_props, uuid=chunk_uuid)


def hybrid_search(
    query_text: str,
    query_vector: list[float] | None = None,
    filters: dict[str, Any] | None = None,
    limit: int = 15,
    alpha: float = 0.5,
    collection_name: str = GOV_CHUNK_COLLECTION,
) -> list[dict[str, Any]]:
    """Execute hybrid search (BM25 + vector).

    Args:
        query_text: Text query for BM25.
        query_vector: Query embedding for vector search.
        filters: Optional metadata filters.
        limit: Max results.
        alpha: Balance between BM25 (0) and vector (1). 0.5 = balanced.
        collection_name: Which collection to search.

    Returns:
        List of result dicts with properties and scores.
    """
    import weaviate.classes.query as wq
    from weaviate.classes.query import Filter

    client = get_client()
    collection = client.collections.get(collection_name)

    weaviate_filter = None
    if filters:
        conditions = []
        if "agency" in filters:
            conditions.append(Filter.by_property("agency").equal(filters["agency"]))
        if "asset" in filters:
            conditions.append(Filter.by_property("asset").equal(filters["asset"]))
        if "level" in filters and collection_name == GOV_CHUNK_COLLECTION:
            conditions.append(Filter.by_property("level").equal(filters["level"]))
        if "doc_ids" in filters:
            conditions.append(Filter.by_property("doc_id").contains_any(filters["doc_ids"]))

        if len(conditions) == 1:
            weaviate_filter = conditions[0]
        elif len(conditions) > 1:
            weaviate_filter = Filter.all_of(conditions)

    results = collection.query.hybrid(
        query=query_text,
        vector=query_vector,
        alpha=alpha,
        limit=limit,
        filters=weaviate_filter,
        return_metadata=wq.MetadataQuery(score=True),
    )

    output = []
    for obj in results.objects:
        result = dict(obj.properties)
        result["_score"] = obj.metadata.score if obj.metadata else 0
        result["_uuid"] = str(obj.uuid)
        output.append(result)

    return output


def sync_from_storage(storage) -> dict[str, int]:
    """Sync all enriched documents and chunks from MinIO to Weaviate."""
    doc_count = 0
    chunk_count = 0

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

                agency, asset = asset_key.split("/")
                parts = obj.object_name.split("/")
                chunks = None
                if len(parts) >= 5:
                    version = f"{parts[3]}/{parts[4]}"
                    chunk_path = f"chunk-zone/{agency}/{asset}/{version}/{asset}_chunks.json"
                    try:
                        chunk_data = storage.get_object(chunk_path)
                        chunk_doc = json.loads(chunk_data.decode("utf-8"))
                        chunks = chunk_doc.get("chunks", [])
                    except Exception:
                        pass

                index_document(enriched_doc, chunks)
                doc_count += 1
                if chunks:
                    chunk_count += len(chunks)

            except Exception as e:
                logger.warning(f"Failed to index {obj.object_name}: {e}")

    except Exception as e:
        logger.error(f"Failed to sync from storage: {e}")

    logger.info(f"Weaviate sync complete: {doc_count} documents, {chunk_count} chunks")
    return {"documents": doc_count, "chunks": chunk_count}


def close() -> None:
    """Close Weaviate client connection."""
    global _client
    if _client is not None:
        _client.close()
        _client = None
