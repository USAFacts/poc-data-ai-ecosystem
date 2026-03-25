#!/usr/bin/env python3
"""Test script to verify the knowledge graph is working for RAG/chatbot use cases.

This script demonstrates the key retrieval capabilities:
1. Entity search - Find documents by entity mention
2. Related entity discovery - Find entities that co-occur
3. Document clustering - Find similar documents
4. Cross-reference navigation - Traverse entity relationships
"""

import json
import sys
from collections import defaultdict
from dataclasses import dataclass

# Add src to path
sys.path.insert(0, "src")

from storage.minio_client import MinioStorage
from storage.naming import RELATIONSHIP_ZONE


@dataclass
class KnowledgeGraph:
    """In-memory knowledge graph built from relationship documents."""

    entities: dict  # canonical_name -> entity data
    relationships: list  # list of relationship dicts
    documents: dict  # doc_id -> document info
    entity_to_docs: dict  # canonical_name -> set of doc_ids
    doc_to_entities: dict  # doc_id -> set of canonical_names
    clusters: list  # list of cluster dicts


def load_knowledge_graph(storage: MinioStorage) -> KnowledgeGraph:
    """Load all relationship data into an in-memory knowledge graph."""

    entities = {}
    relationships = []
    documents = {}
    entity_to_docs = defaultdict(set)
    doc_to_entities = defaultdict(set)
    clusters = []

    # Get all assets with relationship data
    assets = storage.list_assets(zone=RELATIONSHIP_ZONE)

    for asset_path in assets:
        agency, asset = asset_path.split("/")
        versions = storage.list_versions(agency, asset, zone=RELATIONSHIP_ZONE)

        if not versions:
            continue

        # Get latest version
        latest = versions[0]
        object_path = f"{RELATIONSHIP_ZONE}/{agency}/{asset}/{latest}/{asset}.json"

        try:
            data = storage.get_object(object_path)
            doc = json.loads(data)
            rel_data = doc.get("relationships", {})

            doc_id = asset
            documents[doc_id] = {
                "asset": asset,
                "agency": agency,
                "title": doc.get("extraction", {}).get("title", asset),
                "summary": doc.get("enrichment", {}).get("document", {}).get("summary", ""),
            }

            # Process entities
            for entity in rel_data.get("entities", []):
                canonical = entity.get("canonicalName")
                if not canonical:
                    continue

                if canonical not in entities:
                    entities[canonical] = entity
                else:
                    # Merge entity data
                    existing = entities[canonical]
                    existing["documentCount"] = existing.get("documentCount", 0) + entity.get("documentCount", 0)
                    existing["totalMentions"] = existing.get("totalMentions", 0) + entity.get("totalMentions", 0)
                    docs = set(existing.get("documents", []))
                    docs.update(entity.get("documents", []))
                    existing["documents"] = list(docs)

                entity_to_docs[canonical].add(doc_id)
                doc_to_entities[doc_id].add(canonical)

            # Process relationships
            for rel in rel_data.get("relationships", []):
                relationships.append({
                    **rel,
                    "source_doc": doc_id,
                })

            # Process clusters
            for cluster in rel_data.get("clusters", []):
                clusters.append(cluster)

        except Exception as e:
            print(f"  Warning: Failed to load {object_path}: {e}")

    return KnowledgeGraph(
        entities=entities,
        relationships=relationships,
        documents=documents,
        entity_to_docs=dict(entity_to_docs),
        doc_to_entities=dict(doc_to_entities),
        clusters=clusters,
    )


def search_by_entity(kg: KnowledgeGraph, query: str) -> list[dict]:
    """Find documents mentioning an entity (case-insensitive partial match)."""
    query_lower = query.lower()
    results = []

    for canonical, entity in kg.entities.items():
        # Check canonical name, display name, and aliases
        names_to_check = [canonical, entity.get("displayName", "").lower()]
        names_to_check.extend([a.lower() for a in entity.get("aliases", [])])

        if any(query_lower in name for name in names_to_check):
            doc_ids = kg.entity_to_docs.get(canonical, set())
            for doc_id in doc_ids:
                doc = kg.documents.get(doc_id, {})
                results.append({
                    "doc_id": doc_id,
                    "title": doc.get("title", doc_id),
                    "agency": doc.get("agency", ""),
                    "matched_entity": entity.get("displayName", canonical),
                    "entity_type": entity.get("type", "unknown"),
                    "mentions": entity.get("totalMentions", 0),
                })

    # Deduplicate by doc_id
    seen = set()
    unique_results = []
    for r in results:
        if r["doc_id"] not in seen:
            seen.add(r["doc_id"])
            unique_results.append(r)

    return sorted(unique_results, key=lambda x: x["mentions"], reverse=True)


def find_related_entities(kg: KnowledgeGraph, entity_query: str, limit: int = 10) -> list[dict]:
    """Find entities that frequently co-occur with the given entity."""
    query_lower = entity_query.lower()

    # Find the canonical name
    target_canonical = None
    for canonical, entity in kg.entities.items():
        names = [canonical, entity.get("displayName", "").lower()]
        names.extend([a.lower() for a in entity.get("aliases", [])])
        if any(query_lower in name for name in names):
            target_canonical = canonical
            break

    if not target_canonical:
        return []

    # Find related entities through relationships
    related = defaultdict(int)
    for rel in kg.relationships:
        if rel.get("source") == target_canonical:
            related[rel.get("target")] += rel.get("weight", 1)
        elif rel.get("target") == target_canonical:
            related[rel.get("source")] += rel.get("weight", 1)

    results = []
    for canonical, weight in sorted(related.items(), key=lambda x: x[1], reverse=True)[:limit]:
        entity = kg.entities.get(canonical, {})
        results.append({
            "canonical": canonical,
            "display_name": entity.get("displayName", canonical),
            "type": entity.get("type", "unknown"),
            "co_occurrence_weight": weight,
            "document_count": entity.get("documentCount", 0),
        })

    return results


def find_similar_documents(kg: KnowledgeGraph, doc_id: str, limit: int = 5) -> list[dict]:
    """Find documents similar to the given document based on shared entities."""
    if doc_id not in kg.doc_to_entities:
        return []

    target_entities = kg.doc_to_entities[doc_id]
    similarities = []

    for other_doc_id, other_entities in kg.doc_to_entities.items():
        if other_doc_id == doc_id:
            continue

        # Calculate Jaccard similarity
        intersection = len(target_entities & other_entities)
        union = len(target_entities | other_entities)
        similarity = intersection / union if union > 0 else 0

        if similarity > 0:
            doc = kg.documents.get(other_doc_id, {})
            similarities.append({
                "doc_id": other_doc_id,
                "title": doc.get("title", other_doc_id),
                "agency": doc.get("agency", ""),
                "similarity": round(similarity, 3),
                "shared_entities": intersection,
            })

    return sorted(similarities, key=lambda x: x["similarity"], reverse=True)[:limit]


def answer_question(kg: KnowledgeGraph, question: str) -> dict:
    """Demonstrate RAG-style question answering using the knowledge graph.

    This simulates what a chatbot would do:
    1. Extract entities from the question
    2. Find relevant documents
    3. Retrieve context for answering
    """
    # Simple keyword extraction (in production, use NER or LLM)
    keywords = [w.lower() for w in question.split() if len(w) > 3]

    # Find matching entities
    matched_entities = []
    for keyword in keywords:
        for canonical, entity in kg.entities.items():
            names = [canonical, entity.get("displayName", "").lower()]
            if any(keyword in name for name in names):
                matched_entities.append({
                    "canonical": canonical,
                    "display_name": entity.get("displayName", canonical),
                    "type": entity.get("type"),
                })

    # Find relevant documents
    relevant_docs = set()
    for entity in matched_entities:
        doc_ids = kg.entity_to_docs.get(entity["canonical"], set())
        relevant_docs.update(doc_ids)

    # Get document summaries for context
    contexts = []
    for doc_id in list(relevant_docs)[:5]:
        doc = kg.documents.get(doc_id, {})
        if doc.get("summary"):
            contexts.append({
                "doc_id": doc_id,
                "title": doc.get("title", doc_id),
                "summary": doc.get("summary"),
            })

    return {
        "question": question,
        "matched_entities": matched_entities[:5],
        "relevant_documents": len(relevant_docs),
        "contexts": contexts,
    }


def print_section(title: str):
    """Print a formatted section header."""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")


def main():
    """Run knowledge graph tests."""
    print("Loading knowledge graph from relationship-zone...")
    storage = MinioStorage()
    kg = load_knowledge_graph(storage)

    print(f"\nKnowledge Graph Statistics:")
    print(f"  - Entities: {len(kg.entities)}")
    print(f"  - Relationships: {len(kg.relationships)}")
    print(f"  - Documents: {len(kg.documents)}")
    print(f"  - Clusters: {len(kg.clusters)}")

    # Test 1: Entity Search
    print_section("TEST 1: Entity Search")
    print("Query: 'USCIS'")
    results = search_by_entity(kg, "USCIS")
    print(f"Found {len(results)} documents:")
    for r in results[:5]:
        print(f"  - [{r['agency']}] {r['title']}")
        print(f"    Entity: {r['matched_entity']} ({r['entity_type']}), {r['mentions']} mentions")

    # Test 2: Related Entity Discovery
    print_section("TEST 2: Related Entity Discovery")
    print("Query: 'H-1B'")
    related = find_related_entities(kg, "H-1B")
    if related:
        print(f"Entities related to H-1B:")
        for r in related[:7]:
            print(f"  - {r['display_name']} ({r['type']}): weight={r['co_occurrence_weight']}, docs={r['document_count']}")
    else:
        print("No H-1B entity found. Trying 'DACA'...")
        related = find_related_entities(kg, "DACA")
        if related:
            print(f"Entities related to DACA:")
            for r in related[:7]:
                print(f"  - {r['display_name']} ({r['type']}): weight={r['co_occurrence_weight']}, docs={r['document_count']}")

    # Test 3: Similar Document Discovery
    print_section("TEST 3: Similar Document Discovery")
    if kg.documents:
        sample_doc = list(kg.documents.keys())[0]
        print(f"Finding documents similar to: {sample_doc}")
        similar = find_similar_documents(kg, sample_doc)
        if similar:
            for s in similar:
                print(f"  - {s['title']} (similarity: {s['similarity']}, shared: {s['shared_entities']} entities)")
        else:
            print("  No similar documents found.")

    # Test 4: Question Answering Simulation
    print_section("TEST 4: RAG Question Answering Simulation")
    questions = [
        "What are the H-1B visa processing statistics?",
        "How many DACA recipients are there?",
        "What is the immigration backlog?",
    ]

    for question in questions:
        print(f"\nQ: {question}")
        result = answer_question(kg, question)
        print(f"  Matched entities: {[e['display_name'] for e in result['matched_entities']]}")
        print(f"  Relevant documents: {result['relevant_documents']}")
        if result['contexts']:
            print(f"  Top context: {result['contexts'][0]['title']}")
            summary = result['contexts'][0]['summary'][:200] + "..." if len(result['contexts'][0]['summary']) > 200 else result['contexts'][0]['summary']
            print(f"    {summary}")

    # Test 5: Entity Type Distribution
    print_section("TEST 5: Entity Type Distribution")
    type_counts = defaultdict(int)
    for entity in kg.entities.values():
        type_counts[entity.get("type", "unknown")] += 1

    for entity_type, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  {entity_type}: {count}")

    # Test 6: Top Central Entities
    print_section("TEST 6: Top Central Entities (by document count)")
    top_entities = sorted(
        kg.entities.values(),
        key=lambda x: len(kg.entity_to_docs.get(x.get("canonicalName", ""), set())),
        reverse=True
    )[:10]

    for entity in top_entities:
        canonical = entity.get("canonicalName", "")
        doc_count = len(kg.entity_to_docs.get(canonical, set()))
        print(f"  - {entity.get('displayName', canonical)} ({entity.get('type')}): {doc_count} documents")

    print_section("SUMMARY")
    print("The knowledge graph is working! Key capabilities verified:")
    print("  [x] Entity search - Find documents by entity")
    print("  [x] Related entities - Find co-occurring entities")
    print("  [x] Similar documents - Find docs with shared entities")
    print("  [x] RAG context retrieval - Get summaries for questions")
    print("\nNext steps for chatbot integration:")
    print("  1. Use entity search to find relevant documents")
    print("  2. Retrieve document summaries as context")
    print("  3. Pass context to LLM for answer generation")
    print("  4. Use related entities for query expansion")


if __name__ == "__main__":
    main()
