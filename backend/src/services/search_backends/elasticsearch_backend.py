"""Elasticsearch search backend (polyglot persistence).

Uses Elasticsearch for both full-text search (BM25) and vector
similarity search (kNN), keeping the search concern in a
dedicated engine while PostgreSQL handles relational data.
"""

import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", "gov-documents")


def _get_es_client():
    """Lazy-load Elasticsearch client."""
    from elasticsearch import Elasticsearch

    return Elasticsearch(ELASTICSEARCH_URL)


class ElasticsearchSearchBackend:
    """Polyglot persistence: Elasticsearch for search, PostgreSQL for relational data.

    - Semantic search: Elasticsearch kNN with dense_vector field
    - Full-text search: Elasticsearch BM25 with custom analyzers
    - Hybrid: RRF (Reciprocal Rank Fusion) combining both
    - Storage: Documents stored in Elasticsearch + MinIO for raw files
    """

    def __init__(self, dimension: int = 384):
        self._dimension = dimension
        self._client = None

    def _get_client(self):
        if self._client is None:
            self._client = _get_es_client()
        return self._client

    @property
    def name(self) -> str:
        return "Elasticsearch (Polyglot)"

    @property
    def backend_type(self) -> str:
        return "elasticsearch"

    @property
    def persistence_strategy(self) -> str:
        return "polyglot"

    def _ensure_index(self) -> None:
        """Create index with mappings if it doesn't exist."""
        client = self._get_client()
        if client.indices.exists(index=ELASTICSEARCH_INDEX):
            return

        mappings = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "analysis": {
                    "analyzer": {
                        "gov_analyzer": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": ["lowercase", "stop", "snowball"],
                        }
                    }
                },
            },
            "mappings": {
                "properties": {
                    "doc_id": {"type": "keyword"},
                    "title": {
                        "type": "text",
                        "analyzer": "gov_analyzer",
                        "fields": {"keyword": {"type": "keyword"}},
                    },
                    "summary": {"type": "text", "analyzer": "gov_analyzer"},
                    "content": {"type": "text", "analyzer": "gov_analyzer"},
                    "topics": {"type": "text", "analyzer": "gov_analyzer"},
                    "entities": {"type": "text", "analyzer": "gov_analyzer"},
                    "agency": {"type": "keyword"},
                    "asset_name": {"type": "keyword"},
                    "file_format": {"type": "keyword"},
                    "embedding": {
                        "type": "dense_vector",
                        "dims": self._dimension,
                        "index": True,
                        "similarity": "cosine",
                    },
                    "document_json": {"type": "object", "enabled": False},
                    "metadata": {"type": "object", "enabled": False},
                    "indexed_at": {"type": "date"},
                },
            },
        }

        client.indices.create(index=ELASTICSEARCH_INDEX, body=mappings)
        logger.info(f"[elasticsearch] Created index: {ELASTICSEARCH_INDEX}")

    def _extract_text_fields(self, document: dict[str, Any]) -> dict[str, str]:
        """Extract searchable text fields from enriched document."""
        enrichment = document.get("enrichment", {})
        doc_info = enrichment.get("document", {})
        metadata = document.get("metadata", {})
        content = document.get("content", {})
        source = document.get("source", {})

        # Build content from sections
        sections = content.get("sections", [])
        section_texts = [s.get("content", "") for s in sections[:15]]

        # Build entity text
        entities = doc_info.get("entities", [])
        entity_texts = [
            e.get("canonicalName") or e.get("text", "") for e in entities
        ]

        # Build topic text
        topics = doc_info.get("keyTopics", [])
        topic_texts = [t if isinstance(t, str) else t.get("name", "") for t in topics]

        return {
            "title": metadata.get("title", ""),
            "summary": doc_info.get("summary", ""),
            "content": " ".join(section_texts),
            "topics": " ".join(topic_texts),
            "entities": " ".join(entity_texts),
            "agency": source.get("agency", ""),
            "asset_name": source.get("asset", metadata.get("asset_name", "")),
            "file_format": metadata.get("format", ""),
        }

    def index_document(
        self,
        doc_id: str,
        document: dict[str, Any],
        embedding: list[float] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self._ensure_index()
        client = self._get_client()

        text_fields = self._extract_text_fields(document)
        body = {
            "doc_id": doc_id,
            **text_fields,
            "document_json": document,
            "metadata": metadata or {},
            "indexed_at": "now",
        }
        if embedding and len(embedding) == self._dimension:
            body["embedding"] = embedding

        client.index(index=ELASTICSEARCH_INDEX, id=doc_id, body=body)

    def index_documents_batch(
        self,
        doc_ids: list[str],
        documents: list[dict[str, Any]],
        embeddings: list[list[float]] | None = None,
        metadata: list[dict[str, Any]] | None = None,
    ) -> int:
        self._ensure_index()
        client = self._get_client()

        actions = []
        for i, (doc_id, document) in enumerate(zip(doc_ids, documents)):
            text_fields = self._extract_text_fields(document)
            body = {
                "doc_id": doc_id,
                **text_fields,
                "document_json": document,
                "metadata": (metadata[i] if metadata else {}),
                "indexed_at": "now",
            }
            emb = embeddings[i] if embeddings and i < len(embeddings) else None
            if emb and len(emb) == self._dimension:
                body["embedding"] = emb

            actions.append({"index": {"_index": ELASTICSEARCH_INDEX, "_id": doc_id}})
            actions.append(body)

        if not actions:
            return 0

        from elasticsearch.helpers import bulk

        success, errors = bulk(client, self._iter_actions(doc_ids, documents, embeddings, metadata))
        logger.info(f"[elasticsearch] Bulk indexed {success} documents, {len(errors)} errors")
        return success

    def _iter_actions(self, doc_ids, documents, embeddings, metadata):
        """Generate bulk index actions."""
        for i, (doc_id, document) in enumerate(zip(doc_ids, documents)):
            text_fields = self._extract_text_fields(document)
            body = {
                "_index": ELASTICSEARCH_INDEX,
                "_id": doc_id,
                "doc_id": doc_id,
                **text_fields,
                "document_json": document,
                "metadata": (metadata[i] if metadata else {}),
                "indexed_at": "now",
            }
            emb = embeddings[i] if embeddings and i < len(embeddings) else None
            if emb and len(emb) == self._dimension:
                body["embedding"] = emb
            yield body

    def search(
        self,
        query: str,
        query_embedding: list[float] | None = None,
        keywords: list[str] | None = None,
        top_k: int = 5,
    ) -> tuple[list, Any]:
        from src.services.search_backends.base import SearchMetrics, SearchResult

        metrics = SearchMetrics(
            backend_name=self.backend_type,
        )

        client = self._get_client()
        if not client.indices.exists(index=ELASTICSEARCH_INDEX):
            return [], metrics

        # Count total docs
        count_resp = client.count(index=ELASTICSEARCH_INDEX)
        metrics.documents_searched = count_resp["count"]

        # Build hybrid query using sub_searches + RRF
        must_clauses = []

        # BM25 full-text search across all text fields
        bm25_query = {
            "multi_match": {
                "query": query,
                "fields": [
                    "title^3",
                    "summary^2",
                    "entities^2",
                    "topics^1.5",
                    "content",
                ],
                "type": "best_fields",
                "fuzziness": "AUTO",
            }
        }

        # Build the search body
        search_body: dict[str, Any] = {"size": top_k}

        if query_embedding and len(query_embedding) == self._dimension:
            metrics.semantic_search_used = True
            # Hybrid: RRF combining BM25 + kNN
            search_body["query"] = bm25_query
            search_body["knn"] = {
                "field": "embedding",
                "query_vector": query_embedding,
                "k": top_k * 2,
                "num_candidates": min(100, metrics.documents_searched),
            }
            # Use RRF rank fusion if supported (ES 8.8+), else fallback to kNN boost
            search_body["rank"] = {"rrf": {"window_size": top_k * 3}}
        else:
            # Pure BM25
            search_body["query"] = bm25_query

        try:
            response = client.search(index=ELASTICSEARCH_INDEX, body=search_body)
        except Exception as e:
            # Fallback: if RRF not supported, use simple bool query
            logger.warning(f"[elasticsearch] RRF search failed, falling back: {e}")
            search_body.pop("rank", None)
            search_body.pop("knn", None)
            search_body["query"] = bm25_query
            response = client.search(index=ELASTICSEARCH_INDEX, body=search_body)

        results = []
        for hit in response["hits"]["hits"]:
            source = hit["_source"]
            doc = source.get("document_json", {})
            doc_size = len(json.dumps(doc).encode("utf-8"))
            metrics.data_volume_bytes += doc_size

            # Normalize ES score to 0-1 range
            max_score = response["hits"].get("max_score", 1.0) or 1.0
            normalized_score = hit["_score"] / max_score if max_score > 0 else 0.0

            results.append(
                SearchResult(
                    doc_id=hit["_id"],
                    document=doc,
                    score=normalized_score,
                    metadata=source.get("metadata", {}),
                )
            )

        metrics.documents_returned = len(results)
        return results, metrics

    def refresh_index(self, storage) -> int:
        self.clear_index()
        return self._build_from_storage(storage)

    def _build_from_storage(self, storage) -> int:
        """Build index from MinIO enrichment-zone documents."""
        logger.info("[elasticsearch] Building index from storage...")

        doc_ids = []
        documents = []
        embeddings = []
        metadata_list = []

        try:
            enriched_files = storage.list_objects("enrichment-zone/")

            # Deduplicate: latest version per asset
            asset_versions: dict[str, list[tuple[str, str]]] = {}
            for file_path in enriched_files:
                if not file_path.endswith(".json"):
                    continue
                parts = file_path.split("/")
                if len(parts) >= 5:
                    asset_name = parts[2]
                    date_part = parts[3] if len(parts[3]) == 10 else ""
                    time_part = parts[4] if len(parts) >= 6 and len(parts[4]) == 6 else ""
                    timestamp = f"{date_part}-{time_part}" if date_part else ""
                    if asset_name not in asset_versions:
                        asset_versions[asset_name] = []
                    asset_versions[asset_name].append((timestamp, file_path))

            asset_latest: dict[str, str] = {}
            for asset_name, versions in asset_versions.items():
                versions.sort(key=lambda x: x[0], reverse=True)
                asset_latest[asset_name] = versions[0][1]

            for asset_name, file_path in asset_latest.items():
                doc = storage.get_json_object(file_path)
                if not doc:
                    continue

                enrichment = doc.get("enrichment", {})
                embedding_data = enrichment.get("embedding", {})
                vector = embedding_data.get("vector")

                doc_ids.append(file_path)
                documents.append(doc)
                embeddings.append(vector if vector and len(vector) == self._dimension else [])
                metadata_list.append({
                    "asset_name": asset_name,
                    "title": doc.get("metadata", {}).get("title", asset_name),
                    "agency": doc.get("source", {}).get("agency", ""),
                })

            count = self.index_documents_batch(doc_ids, documents, embeddings, metadata_list)
            # Refresh index to make docs searchable immediately
            self._get_client().indices.refresh(index=ELASTICSEARCH_INDEX)
            logger.info(f"[elasticsearch] Indexed {count} documents")
            return count

        except Exception as e:
            logger.error(f"[elasticsearch] Error building index: {e}")
            return 0

    def get_index_status(self) -> dict[str, Any]:
        try:
            client = self._get_client()
            if not client.indices.exists(index=ELASTICSEARCH_INDEX):
                return {
                    "status": "no_index",
                    "documents_indexed": 0,
                    "backend": self.backend_type,
                    "strategy": self.persistence_strategy,
                }

            stats = client.indices.stats(index=ELASTICSEARCH_INDEX)
            doc_count = stats["indices"][ELASTICSEARCH_INDEX]["primaries"]["docs"]["count"]
            store_size = stats["indices"][ELASTICSEARCH_INDEX]["primaries"]["store"]["size_in_bytes"]

            return {
                "status": "ready",
                "documents_indexed": doc_count,
                "store_size_bytes": store_size,
                "backend": self.backend_type,
                "strategy": self.persistence_strategy,
                "elasticsearch_url": ELASTICSEARCH_URL,
            }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "backend": self.backend_type,
                "strategy": self.persistence_strategy,
            }

    def clear_index(self) -> None:
        try:
            client = self._get_client()
            if client.indices.exists(index=ELASTICSEARCH_INDEX):
                client.indices.delete(index=ELASTICSEARCH_INDEX)
                logger.info(f"[elasticsearch] Deleted index: {ELASTICSEARCH_INDEX}")
        except Exception as e:
            logger.error(f"[elasticsearch] Error clearing index: {e}")

    @property
    def document_count(self) -> int:
        try:
            client = self._get_client()
            if not client.indices.exists(index=ELASTICSEARCH_INDEX):
                return 0
            resp = client.count(index=ELASTICSEARCH_INDEX)
            return resp["count"]
        except Exception:
            return 0

    def is_available(self) -> bool:
        try:
            client = self._get_client()
            return client.ping()
        except Exception:
            return False
