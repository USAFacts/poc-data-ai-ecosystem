"""HTML report generator for pipeline status and quality metrics."""

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from control.registry import Registry
from logging_manager import get_logger
from storage.minio_client import MinioStorage
from storage.naming import LANDING_ZONE, PARSED_ZONE, ENRICHMENT_ZONE

logger = get_logger(__name__)


def get_minio_console_url() -> str:
    """Get MinIO console URL from environment or construct from endpoint.

    Returns:
        MinIO console base URL (e.g., http://localhost:9001)
    """
    # First check for explicit console URL
    console_url = os.getenv("MINIO_CONSOLE_URL")
    if console_url:
        return console_url.rstrip("/")

    # Otherwise construct from endpoint
    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    secure = os.getenv("MINIO_SECURE", "false").lower() == "true"
    protocol = "https" if secure else "http"

    # MinIO console is typically on port 9001 (API is 9000)
    if ":9000" in endpoint:
        console_endpoint = endpoint.replace(":9000", ":9001")
    else:
        console_endpoint = f"{endpoint}:9001"

    return f"{protocol}://{console_endpoint}"


def get_minio_object_url(console_url: str, bucket: str, object_path: str) -> str:
    """Build MinIO console browser URL for an object."""
    path_parts = object_path.rsplit("/", 1)
    if len(path_parts) == 2:
        dir_path = path_parts[0]
    else:
        dir_path = object_path

    return f"{console_url}/browser/{bucket}/{dir_path}"


@dataclass
class StepReport:
    """Report data for a single step."""

    name: str
    type: str
    status: str  # "success", "failed", "pending", "not_run"
    zone: str | None = None
    object_path: str | None = None
    object_size: int | None = None
    quality: dict[str, Any] | None = None
    error: str | None = None
    duration_ms: int | None = None
    file_format: str | None = None
    acquisition_method: str | None = None
    parser_type: str | None = None
    page_count: int | None = None  # Pages for PDF, sheets/tabs for Excel
    parse_cost: dict[str, Any] | None = None  # Cost for parsing (e.g., Nanonets credits)
    # Enrichment-specific fields
    enricher_type: str | None = None
    enrichment_model: str | None = None
    enrichment: dict[str, Any] | None = None  # Enrichment data
    entity_count: int | None = None
    topic_count: int | None = None
    table_enrichment_count: int | None = None
    tokens_used: dict[str, int] | None = None
    enrichment_cost: dict[str, Any] | None = None
    enrichment_quality_score: float | None = None  # Calculated enrichment quality 0-100
    # Onboarding-specific fields (implicit step from manifest)
    onboarding_description: str | None = None
    onboarding_labels: dict[str, str] | None = None
    onboarding_source_url: str | None = None
    onboarding_schedule: str | None = None
    onboarding_registered_at: datetime | None = None


@dataclass
class WorkflowReport:
    """Report data for a workflow."""

    name: str
    asset_name: str
    agency_name: str
    agency_full_name: str
    steps: list[StepReport] = field(default_factory=list)
    last_run: datetime | None = None
    last_run_id: str | None = None
    overall_status: str = "not_run"
    quality_score: float | None = None  # Composite: (parse × 0.6) + (enrichment × 0.4)
    parse_quality_score: float | None = None  # Raw parse quality 0-100
    enrichment_quality_score: float | None = None  # Raw enrichment quality 0-100
    total_duration_ms: int | None = None


@dataclass
class StepTiming:
    """Timing data for a single asset with per-step breakdown."""
    asset_name: str
    acquisition_ms: float = 0
    parse_ms: float = 0
    total_ms: float = 0


@dataclass
class QualityData:
    """Quality data for a single asset."""
    asset_name: str
    score: float
    step: str = "composite"  # composite = (parse×0.6 + enrichment×0.4)


@dataclass
class AgencyMetrics:
    """Metrics for a single agency."""
    agency_name: str
    agency_full_name: str
    total_assets: int = 0
    assets_with_onboarding: int = 0
    assets_with_acquisition: int = 0
    assets_with_parse: int = 0
    assets_with_enrichment: int = 0
    successful_workflows: int = 0
    onboarding_coverage: float = 0.0
    acquisition_coverage: float = 0.0
    parse_coverage: float = 0.0
    enrichment_coverage: float = 0.0
    eligible_coverage: float = 0.0  # successful / total onboarded
    avg_quality: float | None = None
    avg_enrichment_quality: float | None = None
    avg_duration_ms: float | None = None
    total_duration_ms: float = 0.0


@dataclass
class DataIngestionScore:
    """Data Ingestion Score (DIS) for a workflow.

    Combines quality, efficiency, and execution success into a single 0-100 score.
    Formula: DIS = (Quality × 0.40) + (Efficiency × 0.30) + (Execution Success × 0.30)
    """
    workflow_name: str
    quality_score: float = 0.0              # 0-100: Composite (parse×0.6 + enrichment×0.4)
    efficiency_score: float = 0.0           # 0-100: Time efficiency (faster = higher)
    execution_success_score: float = 0.0    # 0-100: Step completion percentage
    dis_score: float = 0.0                  # 0-100: Composite score

    # Component details
    has_acquisition: bool = False
    has_parse: bool = False
    duration_ms: float | None = None

    # Trend data (compared to previous pipeline run)
    previous_dis: float | None = None
    trend: float | None = None  # Change from previous (positive = improvement)

    # Component trends
    previous_quality: float | None = None
    quality_trend: float | None = None
    previous_efficiency: float | None = None
    efficiency_trend: float | None = None
    previous_execution_success: float | None = None
    execution_success_trend: float | None = None


@dataclass
class AggregatedEntity:
    """Aggregated entity data across documents."""
    canonical_name: str
    name: str
    type: str  # geography, program, agency, organization, form, etc.
    document_count: int = 0
    documents: list[str] = field(default_factory=list)
    connections: dict[str, int] = field(default_factory=dict)  # entity -> co-occurrence count
    geography_type: str | None = None
    parent_geography: str | None = None


@dataclass
class EntityRelationship:
    """Relationship between two entities based on document co-occurrence."""
    source: str  # canonical_name
    target: str  # canonical_name
    weight: int  # co-occurrence count


@dataclass
class EntityCluster:
    """Cluster of documents sharing common entities."""
    cluster_id: str
    name: str  # Generated from primary entities
    primary_entities: list[str]  # Top 5 entities
    documents: list[str]
    entity_overlap: float  # Jaccard similarity


@dataclass
class EntityGraphData:
    """Complete entity graph data for visualization."""
    entities: list[AggregatedEntity] = field(default_factory=list)
    relationships: list[EntityRelationship] = field(default_factory=list)
    clusters: list[EntityCluster] = field(default_factory=list)
    central_entities: list[tuple[str, int]] = field(default_factory=list)  # (name, connection_count)
    type_distribution: dict[str, int] = field(default_factory=dict)
    total_raw_entities: int = 0
    total_unique_entities: int = 0


@dataclass
class ExecutiveMetrics:
    """Executive dashboard metrics."""

    total_assets: int = 0
    total_workflows: int = 0  # All workflows (including onboarding-only)
    executed_workflows: int = 0  # Workflows with at least acquisition run
    successful_workflows: int = 0  # Workflows fully completed

    # Time metrics - now with per-step breakdown
    avg_duration_ms: float | None = None
    duration_by_format: dict[str, float] = field(default_factory=dict)
    step_timings: list[StepTiming] = field(default_factory=list)  # Per-asset step breakdown
    min_duration_asset: str | None = None
    max_duration_asset: str | None = None

    # Quality metrics - now with step info
    avg_quality: float | None = None
    quality_by_format: dict[str, float] = field(default_factory=dict)
    quality_data: list[QualityData] = field(default_factory=list)  # Per-asset quality with step
    min_quality_asset: str | None = None
    max_quality_asset: str | None = None

    # Coverage metrics
    acquisition_coverage: float = 0.0
    parse_coverage: float = 0.0
    enrichment_coverage: float = 0.0
    assets_with_acquisition: int = 0
    assets_with_parse: int = 0
    assets_with_enrichment: int = 0

    # Parse metrics
    avg_parse_quality: float | None = None
    total_tables_extracted: int = 0
    total_sections_extracted: int = 0
    total_tokens_extracted: int = 0
    document_type_counts: dict[str, int] = field(default_factory=dict)

    # Enrichment metrics
    avg_enrichment_quality: float | None = None
    total_entities_extracted: int = 0
    total_topics_extracted: int = 0
    total_enrichment_cost: float = 0.0
    total_parse_cost: float = 0.0  # Nanonets: 1 page = 1 credit, 100 credits = $1

    # Data Ingestion Score (DIS) metrics
    overall_dis: float = 0.0                                    # Overall DIS across all workflows
    dis_scores: list[DataIngestionScore] = field(default_factory=list)  # Per-workflow DIS
    dis_target_time_ms: float = 300000  # 5 minutes target for efficiency calculation

    # Overall DIS trend
    previous_overall_dis: float | None = None
    overall_dis_trend: float | None = None  # Change from previous (positive = improvement)

    # Agency metrics
    agency_metrics: list[AgencyMetrics] = field(default_factory=list)


def get_latest_version(storage: MinioStorage, zone: str, agency: str, asset: str) -> tuple[str | None, datetime | None, int | None, dict[str, str] | None]:
    """Get the latest version for an asset in a zone."""
    try:
        prefix = f"{zone}/{agency}/{asset}/"
        objects = list(storage.client.list_objects(storage.bucket, prefix=prefix, recursive=True))

        if not objects:
            return None, None, None, None

        latest = max(objects, key=lambda x: x.last_modified)
        object_size = latest.size

        metadata = None
        try:
            stat = storage.client.stat_object(storage.bucket, latest.object_name)
            metadata = stat.metadata
        except Exception:
            pass

        path_parts = latest.object_name.split("/")
        if len(path_parts) >= 5:
            datestamp = path_parts[3]
            timestamp = path_parts[4]
            version = f"{datestamp}/{timestamp}"
            return version, latest.last_modified, object_size, metadata

        return None, latest.last_modified, object_size, metadata

    except Exception:
        return None, None, None, None


def get_parsed_document_data(storage: MinioStorage, agency: str, asset: str, version: str) -> tuple[dict[str, Any] | None, int | None, str | None, int | None]:
    """Get quality metrics, timing, parser type, and page count from a parsed document."""
    try:
        object_path = f"{PARSED_ZONE}/{agency}/{asset}/{version}/{asset}.json"
        data = storage.get_object(object_path)
        doc = json.loads(data)
        quality = doc.get("quality")
        extraction = doc.get("extraction", {})
        processing_time_ms = extraction.get("processingTimeMs")
        parser_type = extraction.get("parser")
        # Page count from source info (pages for PDF, sheets for Excel)
        source = doc.get("source", {})
        page_count = source.get("pageCount")
        return quality, processing_time_ms, parser_type, page_count
    except Exception:
        return None, None, None, None


def get_enriched_document_data(storage: MinioStorage, agency: str, asset: str, version: str) -> tuple[dict[str, Any] | None, int | None, str | None, str | None, int | None, int | None, int | None, dict[str, int] | None, dict[str, Any] | None]:
    """Get enrichment data from an enriched document.

    Returns:
        Tuple of (enrichment, processing_time_ms, enricher_type, model, entity_count, topic_count, table_enrichment_count, tokens_used, cost)
    """
    try:
        object_path = f"{ENRICHMENT_ZONE}/{agency}/{asset}/{version}/{asset}.json"
        data = storage.get_object(object_path)
        doc = json.loads(data)
        enrichment = doc.get("enrichment")
        if not enrichment:
            return None, None, None, None, None, None, None, None, None

        enrichment_info = enrichment.get("enrichmentInfo", {})
        processing_time_ms = enrichment_info.get("processingTimeMs")
        enricher_type = enrichment_info.get("enricher")
        model = enrichment_info.get("model")
        tokens_used = enrichment_info.get("tokensUsed")
        cost = enrichment_info.get("cost")

        # Extract counts from enrichment data
        document = enrichment.get("document", {})
        entity_count = len(document.get("entities", []))
        topic_count = len(document.get("keyTopics", []))
        table_enrichment_count = len(enrichment.get("tables", []))

        return enrichment, processing_time_ms, enricher_type, model, entity_count, topic_count, table_enrichment_count, tokens_used, cost
    except Exception:
        return None, None, None, None, None, None, None, None, None


def calculate_enrichment_quality_score(enrichment: dict[str, Any] | None) -> float | None:
    """Calculate enrichment quality score (0-100) from enrichment data.

    Components (each 0-25 points):
    - Entity Coverage: Based on number and diversity of entities extracted
    - Topic Completeness: Based on number of key topics identified
    - Summary Quality: Based on presence and length of document summary
    - RAG Readiness: Based on example queries and table descriptions

    Returns:
        Quality score 0-100, or None if no enrichment data
    """
    if not enrichment:
        return None

    document = enrichment.get("document", {})
    tables = enrichment.get("tables", [])
    sections = enrichment.get("sections", [])

    # Entity Coverage (0-25 points)
    # 5+ entities = full score, scales linearly
    entities = document.get("entities", [])
    entity_count = len(entities)
    entity_types = len(set(e.get("type", "other") for e in entities)) if entities else 0
    # Score: count contribution (0-15) + diversity contribution (0-10)
    entity_count_score = min(entity_count / 5, 1.0) * 15
    entity_diversity_score = min(entity_types / 3, 1.0) * 10
    entity_score = entity_count_score + entity_diversity_score

    # Topic Completeness (0-25 points)
    # 5+ topics = full score
    topics = document.get("keyTopics", [])
    topic_count = len(topics)
    topic_score = min(topic_count / 5, 1.0) * 25

    # Summary Quality (0-25 points)
    # Based on presence and meaningful length (50+ chars)
    summary = document.get("summary", "")
    if summary:
        # Longer summaries (up to 200 chars) score higher
        length_factor = min(len(summary) / 200, 1.0)
        summary_score = 10 + (length_factor * 15)  # 10 base + up to 15 for length
    else:
        summary_score = 0

    # RAG Readiness (0-25 points)
    # Example queries (0-15) + table/section enrichment (0-10)
    example_queries = document.get("exampleQueries", [])
    query_score = min(len(example_queries) / 3, 1.0) * 15

    # Table descriptions and section summaries
    table_enrichment = sum(1 for t in tables if t.get("description"))
    section_enrichment = sum(1 for s in sections if s.get("summary"))
    enrichment_items = table_enrichment + section_enrichment
    structure_score = min(enrichment_items / 3, 1.0) * 10

    rag_score = query_score + structure_score

    # Total score (0-100)
    total_score = entity_score + topic_score + summary_score + rag_score
    return min(total_score, 100.0)


def aggregate_entity_data(reports: list[WorkflowReport]) -> EntityGraphData:
    """Aggregate entity data across all workflow reports for visualization.

    Builds:
    - Deduplicated entities with document counts
    - Co-occurrence relationships (entities appearing in same document)
    - Document clusters based on shared entities
    - Centrality rankings
    - Type distribution
    """
    result = EntityGraphData()

    # Track entities by canonical name
    entities_map: dict[str, AggregatedEntity] = {}  # canonical_name -> entity
    entities_by_doc: dict[str, set[str]] = {}  # doc_name -> set of canonical_names
    total_raw_count = 0

    # First pass: collect all entities from enrichment steps
    for report in reports:
        for step in report.steps:
            if step.type == "enrichment" and step.enrichment:
                doc_enrichment = step.enrichment.get("document", {})
                entities = doc_enrichment.get("entities", [])
                doc_name = report.asset_name
                doc_entities: set[str] = set()

                for entity in entities[:30]:  # Cap at 30 entities per doc for performance
                    total_raw_count += 1
                    canonical = entity.get("canonicalName") or entity.get("name", "").lower().strip()
                    name = entity.get("name", "Unknown")
                    entity_type = entity.get("type", "other")

                    if not canonical:
                        continue

                    doc_entities.add(canonical)

                    if canonical not in entities_map:
                        entities_map[canonical] = AggregatedEntity(
                            canonical_name=canonical,
                            name=name,
                            type=entity_type,
                            document_count=0,
                            documents=[],
                            connections={},
                            geography_type=entity.get("geographyType"),
                            parent_geography=entity.get("parentGeography"),
                        )

                    agg_entity = entities_map[canonical]
                    if doc_name not in agg_entity.documents:
                        agg_entity.documents.append(doc_name)
                        agg_entity.document_count += 1

                if doc_entities:
                    entities_by_doc[doc_name] = doc_entities

    result.total_raw_entities = total_raw_count
    result.total_unique_entities = len(entities_map)

    # Second pass: build co-occurrence relationships
    relationships_map: dict[tuple[str, str], int] = {}  # (source, target) -> weight

    for doc_name, doc_entities in entities_by_doc.items():
        entity_list = list(doc_entities)
        for i, e1 in enumerate(entity_list):
            for e2 in entity_list[i + 1:]:
                # Ensure consistent ordering for deduplication
                pair = (min(e1, e2), max(e1, e2))
                relationships_map[pair] = relationships_map.get(pair, 0) + 1

                # Update connection counts on entities
                if e1 in entities_map:
                    entities_map[e1].connections[e2] = entities_map[e1].connections.get(e2, 0) + 1
                if e2 in entities_map:
                    entities_map[e2].connections[e1] = entities_map[e2].connections.get(e1, 0) + 1

    # Sort entities by document count and cap at 150 for visualization
    sorted_entities = sorted(entities_map.values(), key=lambda x: x.document_count, reverse=True)[:150]
    result.entities = sorted_entities

    # Filter relationships to only include top entities and cap at 500
    top_entity_names = {e.canonical_name for e in sorted_entities}
    filtered_relationships = [
        EntityRelationship(source=pair[0], target=pair[1], weight=weight)
        for pair, weight in relationships_map.items()
        if pair[0] in top_entity_names and pair[1] in top_entity_names
    ]
    result.relationships = sorted(filtered_relationships, key=lambda x: x.weight, reverse=True)[:500]

    # Calculate central entities (by total connections)
    centrality: list[tuple[str, int]] = []
    for entity in sorted_entities:
        total_connections = sum(entity.connections.values())
        centrality.append((entity.name, total_connections))
    result.central_entities = sorted(centrality, key=lambda x: x[1], reverse=True)[:10]

    # Calculate type distribution
    type_counts: dict[str, int] = {}
    for entity in sorted_entities:
        type_counts[entity.type] = type_counts.get(entity.type, 0) + 1
    result.type_distribution = type_counts

    # Build document clusters using Jaccard similarity
    result.clusters = _cluster_documents(entities_by_doc)

    return result


def _cluster_documents(entities_by_doc: dict[str, set[str]]) -> list[EntityCluster]:
    """Cluster documents based on entity overlap using Jaccard similarity.

    Uses a simple greedy clustering approach:
    1. Start with unclustered documents
    2. Pick a seed document, find all similar docs (Jaccard >= 0.3)
    3. Repeat until all documents are clustered or checked
    """
    if not entities_by_doc:
        return []

    clusters: list[EntityCluster] = []
    unclustered = set(entities_by_doc.keys())
    cluster_id = 0
    similarity_threshold = 0.3

    def jaccard_similarity(set1: set[str], set2: set[str]) -> float:
        if not set1 or not set2:
            return 0.0
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        return intersection / union if union > 0 else 0.0

    while unclustered and cluster_id < 20:  # Cap at 20 clusters
        # Pick a seed document (one with most entities for better clustering)
        seed = max(unclustered, key=lambda d: len(entities_by_doc.get(d, set())))
        seed_entities = entities_by_doc[seed]

        # Find all similar documents
        cluster_docs = [seed]
        cluster_similarities: list[float] = []

        for doc in list(unclustered):
            if doc == seed:
                continue
            similarity = jaccard_similarity(seed_entities, entities_by_doc.get(doc, set()))
            if similarity >= similarity_threshold:
                cluster_docs.append(doc)
                cluster_similarities.append(similarity)

        # Only create cluster if it has at least 2 documents
        if len(cluster_docs) >= 2:
            # Find shared entities across cluster
            shared_entities: dict[str, int] = {}
            for doc in cluster_docs:
                for entity in entities_by_doc.get(doc, set()):
                    shared_entities[entity] = shared_entities.get(entity, 0) + 1

            # Get top 5 entities that appear in most cluster docs
            sorted_shared = sorted(shared_entities.items(), key=lambda x: x[1], reverse=True)[:5]
            primary_entities = [name for name, _ in sorted_shared]

            # Generate cluster name from top 3 entities
            cluster_name = ", ".join(primary_entities[:3]) if primary_entities else f"Cluster {cluster_id + 1}"

            # Calculate average overlap
            avg_overlap = sum(cluster_similarities) / len(cluster_similarities) if cluster_similarities else 0.0

            clusters.append(EntityCluster(
                cluster_id=f"cluster-{cluster_id + 1}",
                name=cluster_name,
                primary_entities=primary_entities,
                documents=cluster_docs,
                entity_overlap=avg_overlap,
            ))

            cluster_id += 1

        # Remove clustered docs from unclustered set
        for doc in cluster_docs:
            unclustered.discard(doc)

    return clusters


def collect_workflow_reports(registry: Registry, storage: MinioStorage) -> list[WorkflowReport]:
    """Collect report data for all workflows and assets without workflows."""
    reports = []

    # Track which assets have workflows
    assets_with_workflows: set[str] = set()

    for wf_name, workflow in registry.workflows.items():
        asset_name = workflow.spec.asset_ref
        asset = registry.assets.get(asset_name)
        if not asset:
            continue

        agency_name = asset.spec.agency_ref
        agency = registry.agencies.get(agency_name)
        if not agency:
            continue

        # Track that this asset has a workflow
        assets_with_workflows.add(asset_name)

        report = WorkflowReport(
            name=wf_name,
            asset_name=asset_name,
            agency_name=agency_name,
            agency_full_name=agency.spec.full_name,
        )

        # Add implicit onboarding step (always first, always success if asset exists)
        onboarding_step = StepReport(
            name="onboarding",
            type="onboarding",
            status="success",  # Always success if we have the asset in registry
            onboarding_description=asset.spec.description if hasattr(asset.spec, 'description') else None,
            onboarding_labels=asset.metadata.labels if hasattr(asset.metadata, 'labels') else None,
            onboarding_source_url=asset.spec.acquisition.source.url if hasattr(asset.spec, 'acquisition') and hasattr(asset.spec.acquisition, 'source') and hasattr(asset.spec.acquisition.source, 'url') else None,
            onboarding_schedule=asset.spec.acquisition.schedule if hasattr(asset.spec, 'acquisition') and hasattr(asset.spec.acquisition, 'schedule') else None,
        )
        report.steps.append(onboarding_step)

        has_success = True  # Onboarding is always success
        has_failure = False
        total_duration_ms = 0
        has_duration = False

        for step_def in workflow.spec.steps:
            step_report = StepReport(
                name=step_def.name,
                type=step_def.type,
                status="not_run",
            )

            if step_def.type == "acquisition":
                version, last_modified, object_size, metadata = get_latest_version(
                    storage, LANDING_ZONE, agency_name, asset_name
                )

                acq_config = asset.spec.acquisition
                step_report.file_format = acq_config.format
                step_report.acquisition_method = acq_config.type.value

                if version:
                    step_report.status = "success"
                    step_report.zone = LANDING_ZONE
                    step_report.object_path = f"{LANDING_ZONE}/{agency_name}/{asset_name}/{version}"
                    step_report.object_size = object_size
                    has_success = True

                    if metadata:
                        duration_str = metadata.get("x-amz-meta-duration_ms") or metadata.get("duration_ms")
                        if duration_str:
                            try:
                                step_report.duration_ms = int(duration_str)
                                total_duration_ms += step_report.duration_ms
                                has_duration = True
                            except (ValueError, TypeError):
                                pass

                    if last_modified and (report.last_run is None or last_modified > report.last_run):
                        report.last_run = last_modified
                        report.last_run_id = version.replace("/", "_")

            elif step_def.type == "parse":
                version, last_modified, object_size, _ = get_latest_version(
                    storage, PARSED_ZONE, agency_name, asset_name
                )
                if version:
                    step_report.status = "success"
                    step_report.zone = PARSED_ZONE
                    step_report.object_path = f"{PARSED_ZONE}/{agency_name}/{asset_name}/{version}"
                    step_report.object_size = object_size
                    has_success = True

                    quality, processing_time_ms, parser_type, page_count = get_parsed_document_data(storage, agency_name, asset_name, version)
                    if quality:
                        step_report.quality = quality
                        report.parse_quality_score = quality.get("scores", {}).get("overall")
                    if processing_time_ms is not None:
                        step_report.duration_ms = processing_time_ms
                        total_duration_ms += processing_time_ms
                        has_duration = True
                    if parser_type:
                        step_report.parser_type = parser_type
                    if page_count is not None:
                        step_report.page_count = page_count
                        # Calculate parse cost for Nanonets: 1 page = 1 credit, 100 credits = $1
                        if parser_type and parser_type.lower() == "nanonets":
                            cost_amount = page_count * 0.01  # $0.01 per page/credit
                            step_report.parse_cost = {
                                "amount": cost_amount,
                                "currency": "USD",
                                "credits": page_count,
                                "rate": "100 credits = $1"
                            }

                    if last_modified and (report.last_run is None or last_modified > report.last_run):
                        report.last_run = last_modified
                        report.last_run_id = version.replace("/", "_")

            elif step_def.type == "enrichment":
                version, last_modified, object_size, _ = get_latest_version(
                    storage, ENRICHMENT_ZONE, agency_name, asset_name
                )
                if version:
                    step_report.status = "success"
                    step_report.zone = ENRICHMENT_ZONE
                    step_report.object_path = f"{ENRICHMENT_ZONE}/{agency_name}/{asset_name}/{version}"
                    step_report.object_size = object_size
                    has_success = True

                    enrichment, processing_time_ms, enricher_type, model, entity_count, topic_count, table_enrichment_count, tokens_used, cost = get_enriched_document_data(
                        storage, agency_name, asset_name, version
                    )
                    if enrichment:
                        step_report.enrichment = enrichment
                    if processing_time_ms is not None:
                        step_report.duration_ms = processing_time_ms
                        total_duration_ms += processing_time_ms
                        has_duration = True
                    if enricher_type:
                        step_report.enricher_type = enricher_type
                    if model:
                        step_report.enrichment_model = model
                    if entity_count is not None:
                        step_report.entity_count = entity_count
                    if topic_count is not None:
                        step_report.topic_count = topic_count
                    if table_enrichment_count is not None:
                        step_report.table_enrichment_count = table_enrichment_count
                    if tokens_used:
                        step_report.tokens_used = tokens_used
                    if cost:
                        step_report.enrichment_cost = cost

                    # Calculate enrichment quality score
                    enrichment_quality = calculate_enrichment_quality_score(enrichment)
                    if enrichment_quality is not None:
                        step_report.enrichment_quality_score = enrichment_quality
                        report.enrichment_quality_score = enrichment_quality

                    if last_modified and (report.last_run is None or last_modified > report.last_run):
                        report.last_run = last_modified
                        report.last_run_id = version.replace("/", "_")

            report.steps.append(step_report)

        if has_duration:
            report.total_duration_ms = total_duration_ms

        if has_success and not has_failure:
            report.overall_status = "success"
        elif has_success and has_failure:
            report.overall_status = "partial"
        elif has_failure:
            report.overall_status = "failed"
        else:
            report.overall_status = "not_run"

        # Calculate composite quality score
        # Formula: (parse × 0.6) + (enrichment × 0.4)
        # When some scores are missing, re-weight available scores proportionally
        scores = []
        weights = []

        if report.parse_quality_score is not None:
            scores.append(report.parse_quality_score)
            weights.append(0.6)
        if report.enrichment_quality_score is not None:
            scores.append(report.enrichment_quality_score)
            weights.append(0.4)

        if scores:
            # Normalize weights to sum to 1.0
            total_weight = sum(weights)
            normalized_weights = [w / total_weight for w in weights]
            report.quality_score = sum(s * w for s, w in zip(scores, normalized_weights))
        else:
            report.quality_score = None

        reports.append(report)

    # Add reports for assets that don't have workflows (onboarding-only)
    for asset_name, asset in registry.assets.items():
        if asset_name in assets_with_workflows:
            continue  # Already processed with a workflow

        agency_name = asset.spec.agency_ref
        agency = registry.agencies.get(agency_name)
        if not agency:
            continue

        # Create a minimal report with only the onboarding step
        report = WorkflowReport(
            name=f"{asset_name}-pending",  # Synthetic workflow name
            asset_name=asset_name,
            agency_name=agency_name,
            agency_full_name=agency.spec.full_name,
            overall_status="not_run",  # No workflow has run yet
        )

        # Add implicit onboarding step
        onboarding_step = StepReport(
            name="onboarding",
            type="onboarding",
            status="success",  # Asset is registered
            onboarding_description=asset.spec.description if hasattr(asset.spec, 'description') else None,
            onboarding_labels=asset.metadata.labels if hasattr(asset.metadata, 'labels') else None,
            onboarding_source_url=asset.spec.acquisition.source.url if hasattr(asset.spec, 'acquisition') and hasattr(asset.spec.acquisition, 'source') and hasattr(asset.spec.acquisition.source, 'url') else None,
            onboarding_schedule=asset.spec.acquisition.schedule if hasattr(asset.spec, 'acquisition') and hasattr(asset.spec.acquisition, 'schedule') else None,
            onboarding_registered_at=asset.metadata.created_at if hasattr(asset.metadata, 'created_at') else None,
        )
        report.steps.append(onboarding_step)

        reports.append(report)

    return reports


def _calculate_efficiency_score(duration_ms: float | None, target_ms: float) -> float:
    """Calculate efficiency score (0-100) based on duration vs target.

    Faster processing = higher score. At target time = 50. Instant = 100.
    Uses logarithmic scale for better distribution.
    """
    if duration_ms is None or duration_ms <= 0:
        return 0.0

    # Ratio of actual time to target time
    ratio = duration_ms / target_ms

    if ratio <= 0:
        return 100.0
    elif ratio >= 2.0:
        # More than 2x target time = minimum score
        return max(0.0, 100.0 - (ratio * 25))
    else:
        # Linear scale: at target (ratio=1) = 75, instant = 100
        return max(0.0, min(100.0, 100.0 - (ratio * 25)))


def calculate_executive_metrics(reports: list[WorkflowReport], registry: Registry) -> ExecutiveMetrics:
    """Calculate executive dashboard metrics."""
    metrics = ExecutiveMetrics()

    metrics.total_assets = len(registry.assets)
    metrics.total_workflows = len(reports)
    # Count workflows that have actually run (at least acquisition completed)
    metrics.executed_workflows = sum(
        1 for r in reports
        if any(s.type == "acquisition" and s.status == "success" for s in r.steps)
    )
    # Successful = all non-onboarding steps completed
    metrics.successful_workflows = sum(
        1 for r in reports
        if any(s.type == "acquisition" and s.status == "success" for s in r.steps)
        and r.overall_status == "success"
    )

    # Collect duration and quality data by format
    duration_by_format: dict[str, list[float]] = {}
    quality_by_format: dict[str, list[float]] = {}
    step_timings: list[StepTiming] = []
    quality_data: list[QualityData] = []

    assets_with_acquisition = set()
    assets_with_parse = set()
    assets_with_enrichment = set()

    # Parse aggregation
    total_tables = 0
    total_sections = 0
    total_tokens = 0
    document_type_counts: dict[str, int] = {}
    parse_quality_scores: list[float] = []

    # Parse cost aggregation
    total_parse_cost = 0.0

    # Enrichment aggregation
    total_entities = 0
    total_topics = 0
    total_enrichment_cost = 0.0

    # Calculate DIS for each workflow
    dis_scores: list[DataIngestionScore] = []

    for report in reports:
        file_format = None
        acquisition_ms = 0.0
        parse_ms = 0.0
        enrichment_ms = 0.0
        has_acquisition = False
        has_parse = False
        has_enrichment = False

        for step in report.steps:
            if step.type == "acquisition":
                file_format = step.file_format or "unknown"
                if step.status == "success":
                    has_acquisition = True
                    assets_with_acquisition.add(report.asset_name)
                    if step.duration_ms is not None:
                        acquisition_ms = float(step.duration_ms)

            if step.type == "parse" and step.status == "success":
                has_parse = True
                assets_with_parse.add(report.asset_name)
                if step.duration_ms is not None:
                    parse_ms = float(step.duration_ms)
                # Collect parse quality metrics
                if step.quality:
                    content = step.quality.get("content", {})
                    total_tables += content.get("tableCount", 0)
                    total_sections += content.get("sectionCount", 0)
                    total_tokens += content.get("estimatedTokens", 0)
                    doc_type = content.get("documentType", "unknown")
                    document_type_counts[doc_type] = document_type_counts.get(doc_type, 0) + 1
                    scores = step.quality.get("scores", {})
                    if scores.get("overall") is not None:
                        parse_quality_scores.append(scores.get("overall"))
                # Collect parse cost (Nanonets)
                if step.parse_cost:
                    total_parse_cost += step.parse_cost.get("amount", 0.0)

            if step.type == "enrichment" and step.status == "success":
                has_enrichment = True
                assets_with_enrichment.add(report.asset_name)
                if step.duration_ms is not None:
                    enrichment_ms = float(step.duration_ms)
                if step.entity_count is not None:
                    total_entities += step.entity_count
                if step.topic_count is not None:
                    total_topics += step.topic_count
                if step.enrichment_cost:
                    total_enrichment_cost += step.enrichment_cost.get("amount", 0.0)

        # Collect step timing data
        total_ms = acquisition_ms + parse_ms + enrichment_ms
        if total_ms > 0:
            step_timings.append(StepTiming(
                asset_name=report.asset_name,
                acquisition_ms=acquisition_ms,
                parse_ms=parse_ms,
                total_ms=total_ms,
            ))
            if file_format:
                if file_format not in duration_by_format:
                    duration_by_format[file_format] = []
                duration_by_format[file_format].append(total_ms)

        # Quality metrics - composite score (parse×0.6 + enrichment×0.4)
        if report.quality_score is not None:
            quality_data.append(QualityData(
                asset_name=report.asset_name,
                score=report.quality_score,
                step="composite",
            ))
            if file_format:
                if file_format not in quality_by_format:
                    quality_by_format[file_format] = []
                quality_by_format[file_format].append(report.quality_score)

        # Calculate Data Ingestion Score (DIS) for this workflow
        # Only include workflows that have progressed beyond onboarding (at least acquisition)
        if has_acquisition:
            # Count expected steps (excluding implicit onboarding)
            expected_steps = len([s for s in report.steps if s.type != "onboarding"])
            completed_steps = sum(1 for s in report.steps if s.status == "success" and s.type != "onboarding")

            # Quality component (40% weight)
            quality_component = report.quality_score if report.quality_score is not None else 0.0

            # Efficiency component (30% weight) - based on processing time
            efficiency_component = _calculate_efficiency_score(
                total_ms if total_ms > 0 else None,
                metrics.dis_target_time_ms
            )

            # Execution success component (30% weight) - step completion percentage
            execution_success_component = (completed_steps / expected_steps * 100) if expected_steps > 0 else 0.0

            # Calculate composite DIS score
            dis_score = (
                (quality_component * 0.40) +
                (efficiency_component * 0.30) +
                (execution_success_component * 0.30)
            )

            dis_scores.append(DataIngestionScore(
                workflow_name=report.name,
                quality_score=quality_component,
                efficiency_score=efficiency_component,
                execution_success_score=execution_success_component,
                dis_score=dis_score,
                has_acquisition=has_acquisition,
                has_parse=has_parse,
                duration_ms=total_ms if total_ms > 0 else None,
            ))

    # Store DIS scores sorted by score (highest first)
    metrics.dis_scores = sorted(dis_scores, key=lambda x: x.dis_score, reverse=True)

    # Calculate overall DIS (average of all workflow DIS scores)
    if metrics.dis_scores:
        metrics.overall_dis = sum(d.dis_score for d in metrics.dis_scores) / len(metrics.dis_scores)

    # Sort and store step timings
    metrics.step_timings = sorted(step_timings, key=lambda x: x.total_ms)
    if metrics.step_timings:
        all_durations = [t.total_ms for t in metrics.step_timings]
        metrics.avg_duration_ms = sum(all_durations) / len(all_durations)
        metrics.min_duration_asset = metrics.step_timings[0].asset_name
        metrics.max_duration_asset = metrics.step_timings[-1].asset_name

    # Sort and store quality data
    metrics.quality_data = sorted(quality_data, key=lambda x: x.score)
    if metrics.quality_data:
        all_qualities = [q.score for q in metrics.quality_data]
        metrics.avg_quality = sum(all_qualities) / len(all_qualities)
        metrics.min_quality_asset = metrics.quality_data[0].asset_name
        metrics.max_quality_asset = metrics.quality_data[-1].asset_name

    # Calculate per-format averages
    for fmt, durations in duration_by_format.items():
        metrics.duration_by_format[fmt] = sum(durations) / len(durations)

    for fmt, qualities in quality_by_format.items():
        metrics.quality_by_format[fmt] = sum(qualities) / len(qualities)

    # Coverage metrics
    metrics.assets_with_acquisition = len(assets_with_acquisition)
    metrics.assets_with_parse = len(assets_with_parse)
    metrics.assets_with_enrichment = len(assets_with_enrichment)

    if metrics.total_assets > 0:
        metrics.acquisition_coverage = (len(assets_with_acquisition) / metrics.total_assets) * 100
        metrics.parse_coverage = (len(assets_with_parse) / metrics.total_assets) * 100
        metrics.enrichment_coverage = (len(assets_with_enrichment) / metrics.total_assets) * 100

    # Parse aggregate metrics
    metrics.total_tables_extracted = total_tables
    metrics.total_sections_extracted = total_sections
    metrics.total_tokens_extracted = total_tokens
    metrics.document_type_counts = document_type_counts
    if parse_quality_scores:
        metrics.avg_parse_quality = sum(parse_quality_scores) / len(parse_quality_scores)

    # Enrichment aggregate metrics
    metrics.total_entities_extracted = total_entities
    metrics.total_topics_extracted = total_topics
    metrics.total_enrichment_cost = total_enrichment_cost
    metrics.total_parse_cost = total_parse_cost

    # Calculate per-agency metrics
    agency_data: dict[str, dict] = {}
    for report in reports:
        agency_name = report.agency_name
        if agency_name not in agency_data:
            agency_data[agency_name] = {
                "full_name": report.agency_full_name,
                "total_assets": 0,
                "successful_workflows": 0,
                "assets_with_onboarding": set(),
                "assets_with_acquisition": set(),
                "assets_with_parse": set(),
                "assets_with_enrichment": set(),
                "quality_scores": [],
                "durations": [],
            }

        agency_data[agency_name]["total_assets"] += 1

        # Check if workflow is successful (has acquisition and overall status is success)
        has_acquisition = any(s.type == "acquisition" and s.status == "success" for s in report.steps)
        if has_acquisition and report.overall_status == "success":
            agency_data[agency_name]["successful_workflows"] += 1

        for step in report.steps:
            if step.type == "onboarding" and step.status == "success":
                agency_data[agency_name]["assets_with_onboarding"].add(report.asset_name)
            if step.type == "acquisition" and step.status == "success":
                agency_data[agency_name]["assets_with_acquisition"].add(report.asset_name)
                if step.duration_ms:
                    agency_data[agency_name]["durations"].append(step.duration_ms)
            if step.type == "parse" and step.status == "success":
                agency_data[agency_name]["assets_with_parse"].add(report.asset_name)
                if step.duration_ms:
                    agency_data[agency_name]["durations"].append(step.duration_ms)
            if step.type == "enrichment" and step.status == "success":
                agency_data[agency_name]["assets_with_enrichment"].add(report.asset_name)
                if step.duration_ms:
                    agency_data[agency_name]["durations"].append(step.duration_ms)

        if report.quality_score is not None:
            agency_data[agency_name]["quality_scores"].append(report.quality_score)

    # Build AgencyMetrics list
    for agency_name, data in sorted(agency_data.items()):
        total = data["total_assets"]
        onboard_count = len(data["assets_with_onboarding"])
        acq_count = len(data["assets_with_acquisition"])
        parse_count = len(data["assets_with_parse"])
        enrich_count = len(data["assets_with_enrichment"])

        successful = data["successful_workflows"]
        metrics.agency_metrics.append(AgencyMetrics(
            agency_name=agency_name,
            agency_full_name=data["full_name"],
            total_assets=total,
            assets_with_onboarding=onboard_count,
            assets_with_acquisition=acq_count,
            assets_with_parse=parse_count,
            assets_with_enrichment=enrich_count,
            successful_workflows=successful,
            onboarding_coverage=(onboard_count / total * 100) if total > 0 else 0.0,
            acquisition_coverage=(acq_count / total * 100) if total > 0 else 0.0,
            parse_coverage=(parse_count / total * 100) if total > 0 else 0.0,
            enrichment_coverage=(enrich_count / total * 100) if total > 0 else 0.0,
            eligible_coverage=(successful / onboard_count * 100) if onboard_count > 0 else 0.0,
            avg_quality=sum(data["quality_scores"]) / len(data["quality_scores"]) if data["quality_scores"] else None,
            avg_duration_ms=sum(data["durations"]) / len(data["durations"]) if data["durations"] else None,
            total_duration_ms=sum(data["durations"]),
        ))

    return metrics


def _load_dis_scores_from_db() -> tuple[dict[str, dict], dict[str, dict], float | None, float | None]:
    """Load current and previous DIS scores from database for trend calculation.

    Since DIS is saved during pipeline execution, we load the actual execution
    scores rather than recomputing them.

    Returns:
        Tuple of:
        - current_scores: workflow_name -> {dis_score, quality, efficiency, execution_success}
        - previous_scores: workflow_name -> {dis_score, quality, efficiency, execution_success}
        - current_overall_dis
        - previous_overall_dis
    """
    try:
        from db.database import get_engine, init_db, get_session
        from sqlalchemy import select, desc
        from db.models import DISHistoryModel, DISOverallHistoryModel

        engine = get_engine()
        init_db(engine)  # Ensure tables exist

        with get_session(engine) as session:
            # Get latest overall DIS
            latest_overall_stmt = (
                select(DISOverallHistoryModel)
                .order_by(desc(DISOverallHistoryModel.recorded_at))
                .limit(1)
            )
            latest_overall = session.execute(latest_overall_stmt).scalar_one_or_none()
            current_overall_dis = latest_overall.overall_dis if latest_overall else None

            # Get second-most-recent overall DIS
            prev_overall_stmt = (
                select(DISOverallHistoryModel)
                .order_by(desc(DISOverallHistoryModel.recorded_at))
                .offset(1)
                .limit(1)
            )
            prev_overall = session.execute(prev_overall_stmt).scalar_one_or_none()
            previous_overall_dis = prev_overall.overall_dis if prev_overall else None

            # Get current and previous DIS for each workflow
            current_scores: dict[str, dict] = {}
            previous_scores: dict[str, dict] = {}

            # Get all unique workflow names
            workflow_names_stmt = select(DISHistoryModel.workflow_name).distinct()
            workflow_names = [row[0] for row in session.execute(workflow_names_stmt).all()]

            for workflow_name in workflow_names:
                # Get latest (current) record
                current_stmt = (
                    select(DISHistoryModel)
                    .where(DISHistoryModel.workflow_name == workflow_name)
                    .order_by(desc(DISHistoryModel.recorded_at))
                    .limit(1)
                )
                current_record = session.execute(current_stmt).scalar_one_or_none()
                if current_record:
                    current_scores[workflow_name] = {
                        "dis_score": current_record.dis_score,
                        "quality_score": current_record.quality_score,
                        "efficiency_score": current_record.efficiency_score,
                        "execution_success_score": current_record.execution_success_score,
                    }

                # Get second-most-recent (previous) record
                prev_stmt = (
                    select(DISHistoryModel)
                    .where(DISHistoryModel.workflow_name == workflow_name)
                    .order_by(desc(DISHistoryModel.recorded_at))
                    .offset(1)
                    .limit(1)
                )
                prev_record = session.execute(prev_stmt).scalar_one_or_none()
                if prev_record:
                    previous_scores[workflow_name] = {
                        "dis_score": prev_record.dis_score,
                        "quality_score": prev_record.quality_score,
                        "efficiency_score": prev_record.efficiency_score,
                        "execution_success_score": prev_record.execution_success_score,
                    }

            return current_scores, previous_scores, current_overall_dis, previous_overall_dis

    except Exception as e:
        logger.debug(f"Could not load DIS scores from database: {e}")
        return {}, {}, None, None


def _save_dis_scores(metrics: ExecutiveMetrics) -> None:
    """Save current DIS scores to database for future trend calculation."""
    try:
        from db.database import get_engine, init_db, get_session
        from db.repository import DISHistoryRepository

        engine = get_engine()
        init_db(engine)  # Ensure tables exist

        with get_session(engine) as session:
            repo = DISHistoryRepository(session)

            # Save per-workflow DIS scores
            for dis in metrics.dis_scores:
                repo.record_workflow_dis(
                    workflow_name=dis.workflow_name,
                    dis_score=dis.dis_score,
                    quality_score=dis.quality_score,
                    efficiency_score=dis.efficiency_score,
                    execution_success_score=dis.execution_success_score,
                )

            # Save overall DIS
            if metrics.dis_scores:
                avg_quality = sum(d.quality_score for d in metrics.dis_scores) / len(metrics.dis_scores)
                avg_efficiency = sum(d.efficiency_score for d in metrics.dis_scores) / len(metrics.dis_scores)
                avg_execution_success = sum(d.execution_success_score for d in metrics.dis_scores) / len(metrics.dis_scores)

                repo.record_overall_dis(
                    overall_dis=metrics.overall_dis,
                    avg_quality=avg_quality,
                    avg_efficiency=avg_efficiency,
                    avg_execution_success=avg_execution_success,
                    workflow_count=len(metrics.dis_scores),
                )

    except Exception as e:
        logger.debug(f"Could not save DIS scores: {e}")


def _apply_dis_from_db(
    metrics: ExecutiveMetrics,
    current_scores: dict[str, dict],
    previous_scores: dict[str, dict],
    current_overall: float | None,
    previous_overall: float | None
) -> None:
    """Calculate DIS trends by comparing freshly calculated scores to previous run.

    Uses freshly calculated scores as current values (not overriding with database),
    and compares to the most recent database values for trend calculation.
    This ensures calculation improvements are reflected immediately.
    """
    # Calculate trends: compare fresh scores to last saved scores
    for dis in metrics.dis_scores:
        workflow_name = dis.workflow_name

        # Use current_scores (last saved run) as the baseline for trend calculation
        # Do NOT override freshly calculated scores with database values
        if workflow_name in current_scores:
            prev = current_scores[workflow_name]
            # Overall DIS trend
            dis.previous_dis = prev["dis_score"]
            dis.trend = dis.dis_score - dis.previous_dis
            # Quality trend
            dis.previous_quality = prev["quality_score"]
            dis.quality_trend = dis.quality_score - dis.previous_quality
            # Efficiency trend
            dis.previous_efficiency = prev["efficiency_score"]
            dis.efficiency_trend = dis.efficiency_score - dis.previous_efficiency
            # Execution success trend
            dis.previous_execution_success = prev["execution_success_score"]
            dis.execution_success_trend = dis.execution_success_score - dis.previous_execution_success

    # Sort by freshly calculated DIS score
    metrics.dis_scores = sorted(metrics.dis_scores, key=lambda x: x.dis_score, reverse=True)

    # Calculate overall DIS from fresh scores (not from database)
    if metrics.dis_scores:
        metrics.overall_dis = sum(d.dis_score for d in metrics.dis_scores) / len(metrics.dis_scores)

    # Apply overall trend (compare fresh overall to last saved overall)
    if current_overall is not None:
        metrics.previous_overall_dis = current_overall
        metrics.overall_dis_trend = metrics.overall_dis - current_overall


def generate_html_report(
    registry: Registry,
    storage: MinioStorage,
    output_path: Path | str | None = None,
) -> str:
    """Generate an HTML report of pipeline status and quality metrics.

    DIS trends are calculated from execution history (saved during pipeline runs),
    not from report generation. This ensures trends reflect actual data changes.

    Args:
        registry: The manifest registry.
        storage: MinIO storage client.
        output_path: Optional path to write the HTML report.

    Returns:
        The generated HTML content.
    """
    reports = collect_workflow_reports(registry, storage)
    metrics = calculate_executive_metrics(reports, registry)
    generated_at = datetime.now(timezone.utc)

    # Load DIS scores from execution history for trend calculation
    # DIS scores are saved during pipeline execution, not report generation
    current_scores, previous_scores, current_overall, previous_overall = _load_dis_scores_from_db()
    _apply_dis_from_db(metrics, current_scores, previous_scores, current_overall, previous_overall)

    console_url = get_minio_console_url()
    bucket = storage.bucket

    html = _generate_html(reports, metrics, generated_at, console_url, bucket)

    if output_path:
        output_path = Path(output_path)
        output_path.write_text(html, encoding="utf-8")

    return html


def _get_status_badge(status: str) -> str:
    """Get HTML badge for a status."""
    colors = {
        "success": "#16a34a",
        "partial": "#ca8a04",
        "failed": "#dc2626",
        "not_run": "#6b7280",
        "pending": "#2563eb",
    }
    color = colors.get(status, "#6b7280")
    label = status.replace("_", " ").title()
    return f'<span class="badge" style="background-color: {color}">{label}</span>'


def _get_quality_badge(score: float | None) -> str:
    """Get HTML badge for a quality score."""
    if score is None:
        return '<span class="badge" style="background-color: #6b7280">N/A</span>'

    if score >= 90:
        color = "#16a34a"
        label = "Excellent"
    elif score >= 70:
        color = "#65a30d"
        label = "Good"
    elif score >= 50:
        color = "#ca8a04"
        label = "Fair"
    else:
        color = "#dc2626"
        label = "Poor"

    return f'<span class="badge" style="background-color: {color}">{score:.1f} ({label})</span>'


def _format_size(size_bytes: int | None) -> str:
    """Format bytes to human-readable size."""
    if size_bytes is None:
        return "N/A"
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"


def _format_duration(duration_ms: int | float | None) -> str:
    """Format duration in milliseconds to human-readable string."""
    if duration_ms is None:
        return "N/A"
    duration_ms = int(duration_ms)
    if duration_ms < 1000:
        return f"{duration_ms}ms"
    elif duration_ms < 60000:
        return f"{duration_ms / 1000:.2f}s"
    else:
        minutes = duration_ms // 60000
        seconds = (duration_ms % 60000) / 1000
        return f"{minutes}m {seconds:.1f}s"


def _format_duration_minutes(duration_ms: float | None) -> str:
    """Format duration in milliseconds to minutes with one decimal place."""
    if duration_ms is None:
        return "N/A"
    minutes = duration_ms / 60000
    return f"{minutes:.1f} min"


def _format_human_datetime(dt: datetime | None) -> str:
    """Format datetime to human-readable string."""
    if dt is None:
        return "Never"

    now = datetime.now(timezone.utc)
    diff = now - dt

    if diff.days == 0:
        hours = diff.seconds // 3600
        if hours == 0:
            minutes = diff.seconds // 60
            if minutes == 0:
                return "Just now"
            elif minutes == 1:
                return "1 minute ago"
            else:
                return f"{minutes} minutes ago"
        elif hours == 1:
            return "1 hour ago"
        else:
            return f"{hours} hours ago"
    elif diff.days == 1:
        return f"Yesterday at {dt.strftime('%H:%M')}"
    elif diff.days < 7:
        return f"{diff.days} days ago"
    else:
        return dt.strftime("%b %d, %Y at %H:%M")


def _generate_html(reports: list[WorkflowReport], metrics: ExecutiveMetrics, generated_at: datetime, console_url: str, bucket: str) -> str:
    """Generate the HTML content."""

    # Aggregate entity data for the relationships page
    entity_data = aggregate_entity_data(reports)

    # Collect unique file formats for filter
    file_formats = set()
    for report in reports:
        for step in report.steps:
            if step.file_format:
                file_formats.add(step.file_format)
    file_formats_list = sorted(file_formats)

    # Generate format options for filter
    format_options = '<option value="all">All Formats</option>'
    for fmt in file_formats_list:
        format_options += f'<option value="{fmt}">{fmt.upper()}</option>'

    # Collect unique agencies for filter
    agencies = set()
    for report in reports:
        if report.agency_name:
            agencies.add((report.agency_name, report.agency_full_name or report.agency_name))
    agencies_list = sorted(agencies, key=lambda x: x[1])

    # Generate agency options for filter
    agency_options = '<option value="all">All Agencies</option>'
    for agency_id, agency_name in agencies_list:
        agency_options += f'<option value="{agency_id}">{agency_name}</option>'

    # Generate latest step options for filter
    latest_step_options = '''
        <option value="all">All Steps</option>
        <option value="onboarding">Onboarding</option>
        <option value="acquisition">Acquisition</option>
        <option value="parse">Parse</option>
        <option value="enrichment">Enrichment</option>
    '''

    # Generate duration by format bars
    duration_bars_html = ""
    if metrics.duration_by_format:
        max_duration = max(metrics.duration_by_format.values()) if metrics.duration_by_format else 1
        for fmt, avg_dur in sorted(metrics.duration_by_format.items()):
            pct = (avg_dur / max_duration) * 100 if max_duration > 0 else 0
            duration_bars_html += f'''
            <div class="breakdown-row">
                <span class="breakdown-label">{fmt.upper()}</span>
                <div class="breakdown-bar-container">
                    <div class="breakdown-bar" style="width: {pct}%; background-color: #3b82f6;"></div>
                </div>
                <span class="breakdown-value">{_format_duration(avg_dur)}</span>
            </div>
            '''

    # Generate quality by format bars
    quality_bars_html = ""
    if metrics.quality_by_format:
        for fmt, avg_qual in sorted(metrics.quality_by_format.items()):
            color = "#16a34a" if avg_qual >= 80 else "#ca8a04" if avg_qual >= 60 else "#dc2626"
            quality_bars_html += f'''
            <div class="breakdown-row">
                <span class="breakdown-label">{fmt.upper()}</span>
                <div class="breakdown-bar-container">
                    <div class="breakdown-bar" style="width: {avg_qual}%; background-color: {color};"></div>
                </div>
                <span class="breakdown-value">{avg_qual:.1f}</span>
            </div>
            '''

    # Generate document type distribution
    doc_type_distribution_html = ""
    if metrics.document_type_counts:
        doc_type_colors = {
            "tabular": "#2563eb",
            "narrative": "#7c3aed",
            "mixed": "#0891b2",
            "unknown": "#6b7280",
        }
        total_docs = sum(metrics.document_type_counts.values())
        doc_type_distribution_html = '''
                <div class="doc-type-distribution">
                    <div class="breakdown-title" style="margin-top: 1rem;">Document Type Distribution</div>
                    <div class="doc-type-badges">
        '''
        for doc_type, count in sorted(metrics.document_type_counts.items(), key=lambda x: -x[1]):
            color = doc_type_colors.get(doc_type, "#6b7280")
            pct = (count / total_docs * 100) if total_docs > 0 else 0
            doc_type_distribution_html += f'''
                        <div class="doc-type-item">
                            <span class="doc-type-badge" style="background-color: {color}">{doc_type.title()}</span>
                            <span class="doc-type-count">{count} ({pct:.0f}%)</span>
                        </div>
            '''
        doc_type_distribution_html += '''
                    </div>
                </div>
        '''

    # Generate chart data for horizontal stacked bars
    # Duration data with per-step breakdown
    duration_labels = [t.asset_name for t in metrics.step_timings]
    acquisition_times = [t.acquisition_ms for t in metrics.step_timings]
    parse_times = [t.parse_ms for t in metrics.step_timings]

    # Quality data with step info
    quality_labels = [q.asset_name for q in metrics.quality_data]
    quality_values = [q.score for q in metrics.quality_data]
    quality_steps = [q.step for q in metrics.quality_data]

    duration_labels_data = json.dumps(duration_labels)
    acquisition_times_data = json.dumps(acquisition_times)
    parse_times_data = json.dumps(parse_times)
    quality_labels_data = json.dumps(quality_labels)
    quality_values_data = json.dumps(quality_values)
    quality_steps_data = json.dumps(quality_steps)

    # Min/max asset info
    min_duration_asset = metrics.min_duration_asset or ""
    max_duration_asset = metrics.max_duration_asset or ""
    min_quality_asset = metrics.min_quality_asset or ""
    max_quality_asset = metrics.max_quality_asset or ""

    # SVG Icons
    icon_clock = '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>'
    icon_star = '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/></svg>'
    icon_chart = '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg>'
    icon_dashboard = '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/></svg>'
    icon_folder = '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/></svg>'
    icon_building = '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M6 22V4a2 2 0 0 1 2-2h8a2 2 0 0 1 2 2v18Z"/><path d="M6 12H4a2 2 0 0 0-2 2v6a2 2 0 0 0 2 2h2"/><path d="M18 9h2a2 2 0 0 1 2 2v9a2 2 0 0 1-2 2h-2"/><path d="M10 6h4"/><path d="M10 10h4"/><path d="M10 14h4"/><path d="M10 18h4"/></svg>'
    icon_network = '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="5" r="3"/><circle cx="5" cy="19" r="3"/><circle cx="19" cy="19" r="3"/><path d="M12 8v4M8.5 16.5l2-2.5M15.5 16.5l-2-2.5"/></svg>'

    # Generate workflow items for assets page
    workflow_items = []
    for idx, report in enumerate(sorted(reports, key=lambda r: r.name)):
        file_format = "unknown"
        for step in report.steps:
            if step.file_format:
                file_format = step.file_format
                break

        steps_html = ""
        for step in report.steps:
            # Build onboarding-specific details (implicit step showing manifest metadata)
            onboarding_details = ""
            if step.type == "onboarding":
                onboarding_details = '<div class="onboarding-info">'
                if step.onboarding_description:
                    onboarding_details += f'''
                <div class="step-detail-row">
                    <span class="detail-label">Description:</span>
                    <span class="detail-value onboarding-description">{step.onboarding_description}</span>
                </div>
                '''
                if step.onboarding_source_url:
                    # Truncate long URLs for display
                    url_display = step.onboarding_source_url if len(step.onboarding_source_url) <= 60 else step.onboarding_source_url[:57] + "..."
                    onboarding_details += f'''
                <div class="step-detail-row">
                    <span class="detail-label">Source:</span>
                    <span class="detail-value"><a href="{step.onboarding_source_url}" target="_blank" class="source-link">{url_display}</a></span>
                </div>
                '''
                if step.onboarding_schedule:
                    onboarding_details += f'''
                <div class="step-detail-row">
                    <span class="detail-label">Schedule:</span>
                    <span class="detail-value"><span class="schedule-badge">{step.onboarding_schedule}</span></span>
                </div>
                '''
                if step.onboarding_labels:
                    labels_html = " ".join([f'<span class="label-tag">{k}: {v}</span>' for k, v in step.onboarding_labels.items()])
                    onboarding_details += f'''
                <div class="step-detail-row labels-row">
                    <span class="detail-label">Labels:</span>
                    <span class="detail-value labels-container">{labels_html}</span>
                </div>
                '''
                onboarding_details += '</div>'

            # Build acquisition-specific details
            acquisition_details = ""
            if step.type == "acquisition" and (step.file_format or step.acquisition_method):
                method_display = (step.acquisition_method or "N/A").upper()
                format_display = (step.file_format or "N/A").upper()
                acquisition_details = f'''
                <div class="step-detail-row">
                    <span class="detail-label">Method:</span>
                    <span class="detail-value"><span class="method-badge">{method_display}</span></span>
                </div>
                <div class="step-detail-row">
                    <span class="detail-label">Format:</span>
                    <span class="detail-value"><span class="format-badge">{format_display}</span></span>
                </div>
                '''

            # Build parse-specific details
            parse_details = ""
            if step.type == "parse" and step.parser_type:
                parser_display = step.parser_type.upper()
                parse_details = f'''
                <div class="step-detail-row">
                    <span class="detail-label">Parser:</span>
                    <span class="detail-value"><span class="parser-badge">{parser_display}</span></span>
                </div>
                '''
                # Add page/sheet count for PDFs and Excel files
                if step.page_count is not None and step.page_count > 0:
                    # Determine label based on file format
                    if file_format in ("xlsx", "xls"):
                        count_label = "Sheets"
                    elif file_format == "pdf":
                        count_label = "Pages"
                    else:
                        count_label = "Pages/Sheets"
                    parse_details += f'''
                <div class="step-detail-row">
                    <span class="detail-label">{count_label}:</span>
                    <span class="detail-value">{step.page_count}</span>
                </div>
                '''
                # Add parse cost (Nanonets)
                if step.parse_cost:
                    cost_amount = step.parse_cost.get("amount", 0)
                    credits = step.parse_cost.get("credits", 0)
                    parse_details += f'''
                <div class="step-detail-row">
                    <span class="detail-label">Cost:</span>
                    <span class="detail-value">${cost_amount:.2f} ({credits} credits)</span>
                </div>
                '''

            # Build enrichment-specific details
            enrichment_details = ""
            if step.type == "enrichment" and step.enricher_type:
                enricher_display = step.enricher_type.upper()
                enrichment_details = f'''
                <div class="step-detail-row">
                    <span class="detail-label">Enricher:</span>
                    <span class="detail-value"><span class="enricher-badge">{enricher_display}</span></span>
                </div>
                '''
                if step.enrichment_model:
                    enrichment_details += f'''
                <div class="step-detail-row">
                    <span class="detail-label">Model:</span>
                    <span class="detail-value">{step.enrichment_model}</span>
                </div>
                '''
                if step.tokens_used:
                    input_tokens = step.tokens_used.get("input", 0)
                    output_tokens = step.tokens_used.get("output", 0)
                    enrichment_details += f'''
                <div class="step-detail-row">
                    <span class="detail-label">Tokens:</span>
                    <span class="detail-value">{input_tokens:,} in / {output_tokens:,} out</span>
                </div>
                '''
                if step.enrichment_cost:
                    cost_amount = step.enrichment_cost.get("amount", 0)
                    cost_currency = step.enrichment_cost.get("currency", "USD")
                    enrichment_details += f'''
                <div class="step-detail-row">
                    <span class="detail-label">Cost:</span>
                    <span class="detail-value">${cost_amount:.4f} {cost_currency}</span>
                </div>
                '''

            # Build storage location
            if step.object_path:
                minio_url = get_minio_object_url(console_url, bucket, step.object_path)
                storage_location = f'<a href="{minio_url}" target="_blank" class="storage-link">{step.object_path}</a>'
            else:
                storage_location = "N/A"

            # Build step details - onboarding is special (no storage/duration)
            if step.type == "onboarding":
                step_details = onboarding_details
            else:
                step_details = f'''
                {acquisition_details}
                {parse_details}
                {enrichment_details}
                <div class="step-detail-row">
                    <span class="detail-label">Duration:</span>
                    <span class="detail-value">{_format_duration(step.duration_ms)}</span>
                </div>
                <div class="step-detail-row">
                    <span class="detail-label">Storage Location:</span>
                    <span class="detail-value monospace">{storage_location}</span>
                </div>
                <div class="step-detail-row">
                    <span class="detail-label">Object Size:</span>
                    <span class="detail-value">{_format_size(step.object_size)}</span>
                </div>
            '''

            # Add quality metrics for parse step
            quality_html = ""
            if step.quality:
                scores = step.quality.get("scores", {})
                content = step.quality.get("content", {})
                tables = step.quality.get("tables", [])
                text_metrics = step.quality.get("text", {})
                doc_type = content.get("documentType", "unknown")

                doc_type_colors = {
                    "tabular": "#2563eb",
                    "narrative": "#7c3aed",
                    "mixed": "#0891b2",
                    "unknown": "#6b7280",
                }
                doc_type_color = doc_type_colors.get(doc_type, "#6b7280")

                quality_html = f'''
                <div class="quality-section">
                    <div class="quality-header">
                        Quality Metrics Parsing
                        <span class="doc-type-badge" style="background-color: {doc_type_color}">{doc_type.title()}</span>
                    </div>
                    <div class="quality-scores">
                        <div class="score-item">
                            <span class="score-label">Overall</span>
                            <span class="score-value">{scores.get("overall", 0):.1f}</span>
                        </div>
                        <div class="score-item">
                            <span class="score-label">Extraction</span>
                            <span class="score-value">{scores.get("extraction", 0):.1f}</span>
                        </div>
                        <div class="score-item">
                            <span class="score-label">Structural</span>
                            <span class="score-value">{scores.get("structural", 0):.1f}</span>
                        </div>
                        <div class="score-item">
                            <span class="score-label">AI Ready</span>
                            <span class="score-value">{scores.get("aiReadiness", 0):.1f}</span>
                        </div>
                    </div>
                    <div class="quality-metrics">
                        <span class="metric">Tokens: {content.get("estimatedTokens", 0):,}</span>
                        <span class="metric">Tables: {content.get("tableCount", 0)}</span>
                        <span class="metric">Sections: {content.get("sectionCount", 0)}</span>
                        <span class="metric">Density: {content.get("contentDensity", 0):.2f}</span>
                    </div>
                </div>
                '''

                if text_metrics and doc_type in ("narrative", "mixed"):
                    formatting = text_metrics.get("formatting", {})
                    format_tags = []
                    if formatting.get("hasBold"):
                        format_tags.append("Bold")
                    if formatting.get("hasItalic"):
                        format_tags.append("Italic")
                    if formatting.get("hasLinks"):
                        format_tags.append("Links")
                    if formatting.get("hasCodeBlocks"):
                        format_tags.append("Code")
                    format_str = ", ".join(format_tags) if format_tags else "None"

                    hierarchy_status = "Valid" if text_metrics.get("headingHierarchyValid") else "Invalid"
                    hierarchy_color = "#16a34a" if text_metrics.get("headingHierarchyValid") else "#ca8a04"

                    quality_html += f'''
                    <div class="text-metrics-section">
                        <div class="text-metrics-header">Text Structure</div>
                        <div class="text-metrics-grid">
                            <div class="text-metric-item">
                                <span class="text-metric-label">Headings</span>
                                <span class="text-metric-value">{text_metrics.get("headingCount", 0)}</span>
                            </div>
                            <div class="text-metric-item">
                                <span class="text-metric-label">Hierarchy</span>
                                <span class="text-metric-value" style="color: {hierarchy_color}">{hierarchy_status}</span>
                            </div>
                            <div class="text-metric-item">
                                <span class="text-metric-label">Paragraphs</span>
                                <span class="text-metric-value">{text_metrics.get("paragraphCount", 0)}</span>
                            </div>
                            <div class="text-metric-item">
                                <span class="text-metric-label">Avg Length</span>
                                <span class="text-metric-value">{text_metrics.get("avgParagraphLength", 0):.0f}</span>
                            </div>
                            <div class="text-metric-item">
                                <span class="text-metric-label">Lists</span>
                                <span class="text-metric-value">{text_metrics.get("listCount", 0)} ({text_metrics.get("listItemsTotal", 0)} items)</span>
                            </div>
                            <div class="text-metric-item">
                                <span class="text-metric-label">Sentences</span>
                                <span class="text-metric-value">{text_metrics.get("sentenceCount", 0)}</span>
                            </div>
                        </div>
                        <div class="text-scores">
                            <span class="metric">Structure: {text_metrics.get("structureScore", 0):.1f}</span>
                            <span class="metric">Completeness: {text_metrics.get("completenessScore", 0):.1f}</span>
                            <span class="metric">Formatting: {format_str}</span>
                        </div>
                    </div>
                    '''

                if tables:
                    table_rows = ""
                    for i, t in enumerate(tables):
                        table_rows += f'''
                        <tr>
                            <td>Table {i + 1}</td>
                            <td>{t.get("rowCount", 0)} x {t.get("columnCount", 0)}</td>
                            <td>{t.get("cellFillRate", 0):.1f}%</td>
                            <td>{t.get("headerQuality", 0):.1f}%</td>
                            <td>{t.get("dataTypeConsistency", 0):.1f}%</td>
                        </tr>
                        '''
                    quality_html += f'''
                    <div class="tables-section">
                        <div class="tables-header">Table Quality</div>
                        <table class="tables-detail">
                            <thead>
                                <tr>
                                    <th>Table</th>
                                    <th>Size</th>
                                    <th>Cell Fill</th>
                                    <th>Headers</th>
                                    <th>Type Consistency</th>
                                </tr>
                            </thead>
                            <tbody>
                                {table_rows}
                            </tbody>
                        </table>
                    </div>
                    '''

            # Add enrichment quality metrics for enrichment step
            enrichment_quality_html = ""
            if step.type == "enrichment" and step.enrichment:
                doc_enrichment = step.enrichment.get("document", {})
                table_enrichments = step.enrichment.get("tables", [])
                enrichment_info = step.enrichment.get("enrichmentInfo", {})

                entity_count = step.entity_count or 0
                topic_count = step.topic_count or 0
                table_enrich_count = step.table_enrichment_count or 0
                summary = doc_enrichment.get("summary", "")[:150]
                if len(doc_enrichment.get("summary", "")) > 150:
                    summary += "..."

                enricher_type = enrichment_info.get("enricher", "unknown")
                enricher_color = "#16a34a" if "llm" in enricher_type.lower() else "#2563eb"

                enrichment_quality_html = f'''
                <div class="quality-section enrichment-quality">
                    <div class="quality-header">
                        Quality Metrics Enrichment
                        <span class="doc-type-badge" style="background-color: {enricher_color}">{enricher_type}</span>
                    </div>
                    <div class="quality-scores">
                        <div class="score-item">
                            <span class="score-label">Entities</span>
                            <span class="score-value">{entity_count}</span>
                        </div>
                        <div class="score-item">
                            <span class="score-label">Topics</span>
                            <span class="score-value">{topic_count}</span>
                        </div>
                        <div class="score-item">
                            <span class="score-label">Tables Enriched</span>
                            <span class="score-value">{table_enrich_count}</span>
                        </div>
                        <div class="score-item">
                            <span class="score-label">Example Queries</span>
                            <span class="score-value">{len(doc_enrichment.get("exampleQueries", []))}</span>
                        </div>
                    </div>
                    <div class="enrichment-summary">
                        <span class="summary-label">Summary:</span>
                        <span class="summary-text">{summary}</span>
                    </div>
                    <div class="quality-metrics">
                        <span class="metric">Doc Type: {doc_enrichment.get("documentType", "N/A")}</span>
                        <span class="metric">Temporal: {doc_enrichment.get("temporalScope", {}).get("period", "N/A") if doc_enrichment.get("temporalScope") else "N/A"}</span>
                        <span class="metric">Audience: {", ".join(doc_enrichment.get("targetAudience", [])[:2]) or "N/A"}</span>
                    </div>
                </div>
                '''

                # Show entities if available
                entities = doc_enrichment.get("entities", [])[:5]
                if entities:
                    entity_items = ""
                    for e in entities:
                        entity_items += f'<span class="entity-tag entity-{e.get("type", "other")}">{e.get("name", "")} <small>({e.get("type", "")})</small></span>'
                    enrichment_quality_html += f'''
                    <div class="entities-section">
                        <div class="entities-header">Top Entities</div>
                        <div class="entity-tags">{entity_items}</div>
                    </div>
                    '''

                # Show key topics if available
                topics = doc_enrichment.get("keyTopics", [])[:6]
                if topics:
                    topic_items = "".join([f'<span class="topic-tag">{t}</span>' for t in topics])
                    enrichment_quality_html += f'''
                    <div class="topics-section">
                        <div class="topics-header">Key Topics</div>
                        <div class="topic-tags">{topic_items}</div>
                    </div>
                    '''

            steps_html += f'''
            <div class="step-card collapsible collapsed">
                <div class="step-header" onclick="toggleStepCard(this)">
                    <div class="step-header-left">
                        <span class="step-toggle-icon">▶</span>
                        <span class="step-name">{step.name.title()}</span>
                        <span class="step-type">({step.type})</span>
                    </div>
                    {_get_status_badge(step.status)}
                </div>
                <div class="step-details">
                    {step_details}
                    {quality_html}
                    {enrichment_quality_html}
                </div>
            </div>
            '''

        last_run_human = _format_human_datetime(report.last_run)
        total_duration_str = _format_duration(report.total_duration_ms)
        duration_badge = f'<span class="duration-badge">{total_duration_str}</span>' if report.total_duration_ms else ''

        # Determine the latest successful step for the progress indicator
        step_order = ["onboarding", "acquisition", "parse", "enrichment"]
        step_colors = {
            "onboarding": "#8b5cf6",
            "acquisition": "#3b82f6",
            "parse": "#7c3aed",
            "enrichment": "#16a34a",
        }
        latest_step = None
        for step in report.steps:
            if step.status == "success" and step.type in step_order:
                if latest_step is None or step_order.index(step.type) > step_order.index(latest_step):
                    latest_step = step.type

        latest_step_badge = ""
        if latest_step:
            step_color = step_colors.get(latest_step, "#6b7280")
            latest_step_badge = f'<span class="latest-step-badge" style="background-color: {step_color}">{latest_step.title()}</span>'

        # Build quality breakdown section if we have quality scores
        quality_breakdown_html = ""
        if report.quality_score is not None:
            parse_score_str = f"{report.parse_quality_score:.1f}" if report.parse_quality_score is not None else "N/A"
            enrich_score_str = f"{report.enrichment_quality_score:.1f}" if report.enrichment_quality_score is not None else "N/A"
            composite_str = f"{report.quality_score:.1f}"

            # Determine quality colors
            def get_quality_color(score):
                if score is None:
                    return "#6b7280"
                if score >= 80:
                    return "#16a34a"
                if score >= 60:
                    return "#ca8a04"
                return "#dc2626"

            parse_color = get_quality_color(report.parse_quality_score)
            enrich_color = get_quality_color(report.enrichment_quality_score)
            composite_color = get_quality_color(report.quality_score)

            quality_breakdown_html = f'''
                <div class="quality-breakdown">
                    <div class="quality-breakdown-title">Quality Score Breakdown</div>
                    <div class="quality-breakdown-grid">
                        <div class="quality-breakdown-item">
                            <span class="breakdown-label">Parse (60%)</span>
                            <span class="breakdown-score" style="color: {parse_color}">{parse_score_str}</span>
                        </div>
                        <div class="quality-breakdown-item">
                            <span class="breakdown-label">Enrichment (40%)</span>
                            <span class="breakdown-score" style="color: {enrich_color}">{enrich_score_str}</span>
                        </div>
                        <div class="quality-breakdown-item composite">
                            <span class="breakdown-label">Composite</span>
                            <span class="breakdown-score" style="color: {composite_color}; font-size: 1.1rem;">{composite_str}</span>
                        </div>
                    </div>
                </div>
            '''

        workflow_items.append(f'''
        <div class="workflow-item" data-format="{file_format}" data-agency="{report.agency_name}" data-latest-step="{latest_step or 'none'}">
            <div class="workflow-header" onclick="toggleWorkflow({idx})">
                <div class="workflow-summary">
                    <span class="expand-icon" id="icon-{idx}">+</span>
                    <div class="workflow-info">
                        <span class="dataset-name">{report.asset_name}</span>
                        <span class="agency-name">{report.agency_full_name}</span>
                    </div>
                </div>
                <div class="workflow-meta">
                    {latest_step_badge}
                    {duration_badge}
                    <span class="last-processed">{last_run_human}</span>
                    {_get_status_badge(report.overall_status)}
                    {_get_quality_badge(report.quality_score) if report.quality_score else ''}
                </div>
            </div>
            <div class="workflow-details" id="details-{idx}">
                <div class="steps-container">
                    {steps_html}
                </div>
                {quality_breakdown_html}
            </div>
        </div>
        ''')

    avg_duration_str = _format_duration_minutes(metrics.avg_duration_ms) if metrics.avg_duration_ms else "N/A"
    avg_quality_str = f"{metrics.avg_quality:.1f}" if metrics.avg_quality else "N/A"
    overall_dis_str = f"{metrics.overall_dis:.1f}" if metrics.overall_dis else "N/A"

    # Generate DIS data for chart
    dis_labels = [d.workflow_name for d in metrics.dis_scores]
    dis_quality = [d.quality_score for d in metrics.dis_scores]
    dis_efficiency = [d.efficiency_score for d in metrics.dis_scores]
    dis_execution_success = [d.execution_success_score for d in metrics.dis_scores]
    dis_total = [d.dis_score for d in metrics.dis_scores]

    dis_labels_data = json.dumps(dis_labels)
    dis_quality_data = json.dumps(dis_quality)
    dis_efficiency_data = json.dumps(dis_efficiency)
    dis_execution_success_data = json.dumps(dis_execution_success)
    dis_total_data = json.dumps(dis_total)

    # Entity graph data for vis.js network
    entity_type_colors = {
        "geography": "#3b82f6",   # blue
        "agency": "#f59e0b",      # amber
        "program": "#10b981",     # green
        "organization": "#8b5cf6", # purple
        "form": "#ec4899",        # pink
        "money": "#06b6d4",       # cyan
        "date": "#eab308",        # yellow
        "other": "#6b7280",       # gray
    }

    # Build nodes for vis.js
    vis_nodes = []
    for entity in entity_data.entities:
        color = entity_type_colors.get(entity.type, "#6b7280")
        size = min(10 + entity.document_count * 5, 40)  # Scale size by doc count
        vis_nodes.append({
            "id": entity.canonical_name,
            "label": entity.name[:20] + "..." if len(entity.name) > 20 else entity.name,
            "title": f"{entity.name} ({entity.type})\\n{entity.document_count} documents",
            "size": size,
            "color": color,
            "type": entity.type,
            "docCount": entity.document_count,
            "fullName": entity.name,
            "documents": entity.documents[:10],  # Limit for performance
        })

    # Build edges for vis.js
    vis_edges = []
    for rel in entity_data.relationships:
        vis_edges.append({
            "from": rel.source,
            "to": rel.target,
            "value": rel.weight,
            "title": f"Co-occurrence: {rel.weight}",
        })

    vis_nodes_data = json.dumps(vis_nodes)
    vis_edges_data = json.dumps(vis_edges)

    # Entity type distribution for doughnut chart
    entity_type_labels = list(entity_data.type_distribution.keys())
    entity_type_counts = list(entity_data.type_distribution.values())
    entity_type_colors_list = [entity_type_colors.get(t, "#6b7280") for t in entity_type_labels]
    entity_type_labels_data = json.dumps(entity_type_labels)
    entity_type_counts_data = json.dumps(entity_type_counts)
    entity_type_colors_data = json.dumps(entity_type_colors_list)

    # Central entities for horizontal bar chart
    central_entity_names = [name for name, _ in entity_data.central_entities]
    central_entity_counts = [count for _, count in entity_data.central_entities]
    central_entity_names_data = json.dumps(central_entity_names)
    central_entity_counts_data = json.dumps(central_entity_counts)

    # Build cluster cards HTML
    clusters_html = ""
    for cluster in entity_data.clusters:
        entity_tags = "".join([
            f'<span class="cluster-entity-tag">{e}</span>'
            for e in cluster.primary_entities[:5]
        ])
        doc_list = ", ".join(cluster.documents[:5])
        if len(cluster.documents) > 5:
            doc_list += f" (+{len(cluster.documents) - 5} more)"

        clusters_html += f'''
        <div class="cluster-card">
            <div class="cluster-header">
                <span class="cluster-name">{cluster.name[:50]}{"..." if len(cluster.name) > 50 else ""}</span>
                <span class="cluster-doc-count">{len(cluster.documents)} documents</span>
            </div>
            <div class="cluster-entities">{entity_tags}</div>
            <div class="cluster-overlap">Entity overlap: {cluster.entity_overlap * 100:.0f}%</div>
            <div class="cluster-docs">Documents: {doc_list}</div>
        </div>
        '''

    # DIS color based on score
    def get_dis_color(score: float) -> str:
        if score >= 80:
            return "green"
        elif score >= 60:
            return "yellow"
        else:
            return "red"

    dis_color = get_dis_color(metrics.overall_dis) if metrics.overall_dis else "text-muted"

    # Overall DIS trend indicator
    def get_trend_html(trend: float | None, compact: bool = False) -> str:
        if trend is None:
            return '<span class="trend-na">—</span>' if not compact else ''
        if abs(trend) < 0.1:
            return '<span class="trend-neutral">→ 0.0</span>' if not compact else '<span class="trend-neutral">→</span>'
        elif trend > 0:
            return f'<span class="trend-up">↑ +{trend:.1f}</span>' if not compact else f'<span class="trend-up">↑ +{trend:.1f}</span>'
        else:
            return f'<span class="trend-down">↓ {trend:.1f}</span>' if not compact else f'<span class="trend-down">↓ {trend:.1f}</span>'

    overall_dis_trend_html = get_trend_html(metrics.overall_dis_trend, compact=True)

    # Helper for compact trend (just arrow, no number)
    def get_trend_compact(trend: float | None) -> str:
        if trend is None:
            return ''
        if abs(trend) < 0.1:
            return '<span class="trend-neutral" title="No change">→</span>'
        elif trend > 0:
            return f'<span class="trend-up" title="+{trend:.1f}">↑</span>'
        else:
            return f'<span class="trend-down" title="{trend:.1f}">↓</span>'

    # Generate per-workflow DIS rows for table
    dis_table_rows = ""
    for dis in metrics.dis_scores:
        score_color = "#16a34a" if dis.dis_score >= 80 else "#ca8a04" if dis.dis_score >= 60 else "#dc2626"
        trend_html = get_trend_html(dis.trend)
        quality_trend_html = get_trend_compact(dis.quality_trend)
        efficiency_trend_html = get_trend_compact(dis.efficiency_trend)
        exec_success_trend_html = get_trend_compact(dis.execution_success_trend)
        dis_table_rows += f'''
        <tr>
            <td>{dis.workflow_name}</td>
            <td style="color: {score_color}; font-weight: 600;">{dis.dis_score:.1f}</td>
            <td>{trend_html}</td>
            <td>{dis.quality_score:.1f} {quality_trend_html}</td>
            <td>{dis.efficiency_score:.1f} {efficiency_trend_html}</td>
            <td>{dis.execution_success_score:.1f} {exec_success_trend_html}</td>
        </tr>
        '''

    # Generate agency cards HTML
    agency_cards_html = ""
    for agency in metrics.agency_metrics:
        quality_str = f"{agency.avg_quality:.1f}" if agency.avg_quality else "N/A"
        quality_color = "#16a34a" if agency.avg_quality and agency.avg_quality >= 80 else "#ca8a04" if agency.avg_quality and agency.avg_quality >= 60 else "#6b7280"
        duration_str = _format_duration(agency.avg_duration_ms) if agency.avg_duration_ms else "N/A"

        agency_cards_html += f'''
        <div class="agency-card">
            <div class="agency-header">
                <div class="agency-name">{agency.agency_full_name}</div>
                <div class="agency-code">{agency.agency_name}</div>
            </div>
            <div class="agency-stats">
                <div class="agency-stat">
                    <div class="stat-value">{agency.total_assets}</div>
                    <div class="stat-label">Assets</div>
                </div>
                <div class="agency-stat">
                    <div class="stat-value">{agency.onboarding_coverage:.0f}%</div>
                    <div class="stat-label">Onboarded</div>
                </div>
                <div class="agency-stat">
                    <div class="stat-value">{agency.acquisition_coverage:.0f}%</div>
                    <div class="stat-label">Acquired</div>
                </div>
                <div class="agency-stat">
                    <div class="stat-value">{agency.parse_coverage:.0f}%</div>
                    <div class="stat-label">Parsed</div>
                </div>
                <div class="agency-stat">
                    <div class="stat-value">{agency.enrichment_coverage:.0f}%</div>
                    <div class="stat-label">Enriched</div>
                </div>
                <div class="agency-stat">
                    <div class="stat-value" style="color: var(--accent-purple)">{agency.eligible_coverage:.0f}%</div>
                    <div class="stat-label">Eligible</div>
                </div>
                <div class="agency-stat">
                    <div class="stat-value" style="color: {quality_color}">{quality_str}</div>
                    <div class="stat-label">Avg Quality</div>
                </div>
            </div>
            <div class="agency-coverage-bars">
                <div class="coverage-bar-row">
                    <span class="coverage-bar-label">Onboarding</span>
                    <div class="coverage-bar-track">
                        <div class="coverage-bar-fill onboarding" style="width: {agency.onboarding_coverage}%"></div>
                    </div>
                    <span class="coverage-bar-value">{agency.assets_with_onboarding}/{agency.total_assets}</span>
                </div>
                <div class="coverage-bar-row">
                    <span class="coverage-bar-label">Acquisition</span>
                    <div class="coverage-bar-track">
                        <div class="coverage-bar-fill acquisition" style="width: {agency.acquisition_coverage}%"></div>
                    </div>
                    <span class="coverage-bar-value">{agency.assets_with_acquisition}/{agency.total_assets}</span>
                </div>
                <div class="coverage-bar-row">
                    <span class="coverage-bar-label">Parse</span>
                    <div class="coverage-bar-track">
                        <div class="coverage-bar-fill parse" style="width: {agency.parse_coverage}%"></div>
                    </div>
                    <span class="coverage-bar-value">{agency.assets_with_parse}/{agency.total_assets}</span>
                </div>
                <div class="coverage-bar-row">
                    <span class="coverage-bar-label">Enrichment</span>
                    <div class="coverage-bar-track">
                        <div class="coverage-bar-fill enrichment" style="width: {agency.enrichment_coverage}%"></div>
                    </div>
                    <span class="coverage-bar-value">{agency.assets_with_enrichment}/{agency.total_assets}</span>
                </div>
                <div class="coverage-bar-row">
                    <span class="coverage-bar-label">Eligible</span>
                    <div class="coverage-bar-track">
                        <div class="coverage-bar-fill" style="width: {agency.eligible_coverage}%; background: var(--accent-purple);"></div>
                    </div>
                    <span class="coverage-bar-value">{agency.successful_workflows}/{agency.assets_with_onboarding}</span>
                </div>
            </div>
        </div>
        '''

    # SVG icon for DIS
    icon_gauge = '<svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 2v4M12 18v4M4.93 4.93l2.83 2.83M16.24 16.24l2.83 2.83M2 12h4M18 12h4M4.93 19.07l2.83-2.83M16.24 7.76l2.83-2.83"/><circle cx="12" cy="12" r="3"/></svg>'

    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Pipeline Status Report</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        :root {{
            --bg-primary: #f8fafc;
            --bg-secondary: #ffffff;
            --bg-card: #f1f5f9;
            --bg-sidebar: #1e293b;
            --text-primary: #1e293b;
            --text-secondary: #475569;
            --text-muted: #94a3b8;
            --text-sidebar: #f8fafc;
            --border-color: #e2e8f0;
            --accent-green: #16a34a;
            --accent-yellow: #ca8a04;
            --accent-red: #dc2626;
            --accent-blue: #2563eb;
            --accent-purple: #7c3aed;
        }}

        * {{
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background-color: var(--bg-primary);
            color: var(--text-primary);
            line-height: 1.6;
            display: flex;
            min-height: 100vh;
        }}

        /* Sidebar Navigation */
        .sidebar {{
            width: 240px;
            background-color: var(--bg-sidebar);
            color: var(--text-sidebar);
            padding: 1.5rem;
            position: fixed;
            height: 100vh;
            overflow-y: auto;
        }}

        .sidebar-logo {{
            font-size: 1.25rem;
            font-weight: 700;
            margin-bottom: 0.5rem;
            color: white;
        }}

        .sidebar-subtitle {{
            font-size: 0.75rem;
            color: var(--text-muted);
            margin-bottom: 2rem;
        }}

        .nav-section {{
            margin-bottom: 1.5rem;
        }}

        .nav-section-title {{
            font-size: 0.7rem;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            color: var(--text-muted);
            margin-bottom: 0.75rem;
        }}

        .nav-item {{
            display: flex;
            align-items: center;
            gap: 0.75rem;
            padding: 0.75rem 1rem;
            border-radius: 0.5rem;
            cursor: pointer;
            transition: background-color 0.15s ease;
            color: var(--text-sidebar);
            text-decoration: none;
            margin-bottom: 0.25rem;
        }}

        .nav-item:hover {{
            background-color: rgba(255, 255, 255, 0.1);
        }}

        .nav-item.active {{
            background-color: var(--accent-blue);
        }}

        .nav-item .icon {{
            width: 18px;
            height: 18px;
            flex-shrink: 0;
        }}

        .metrics-card h3 .icon {{
            width: 20px;
            height: 20px;
            vertical-align: middle;
            margin-right: 0.25rem;
        }}

        /* Main Content */
        .main-content {{
            margin-left: 240px;
            flex: 1;
            padding: 2rem;
            min-height: 100vh;
        }}

        .page {{
            display: none;
        }}

        .page.active {{
            display: block;
        }}

        .page-header {{
            margin-bottom: 2rem;
        }}

        .page-header h1 {{
            font-size: 1.75rem;
            font-weight: 600;
            margin-bottom: 0.25rem;
        }}

        .page-header p {{
            color: var(--text-secondary);
            font-size: 0.875rem;
        }}

        /* Summary Cards */
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 1rem;
            margin-bottom: 2rem;
        }}

        .summary-card {{
            background-color: var(--bg-secondary);
            border-radius: 0.75rem;
            padding: 1.25rem;
            border: 1px solid var(--border-color);
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        }}

        .summary-card h4 {{
            color: var(--text-secondary);
            font-size: 0.75rem;
            font-weight: 500;
            margin-bottom: 0.5rem;
            text-transform: uppercase;
            letter-spacing: 0.025em;
        }}

        .summary-card .value {{
            font-size: 1.75rem;
            font-weight: 700;
        }}

        .summary-card .value.green {{ color: var(--accent-green); }}
        .summary-card .value.yellow {{ color: var(--accent-yellow); }}
        .summary-card .value.blue {{ color: var(--accent-blue); }}
        .summary-card .value.purple {{ color: var(--accent-purple); }}

        .summary-card .sublabel {{
            font-size: 0.7rem;
            color: var(--text-muted);
            margin-top: 0.25rem;
        }}

        /* Metrics Sections */
        .metrics-row {{
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 1.5rem;
            margin-bottom: 2rem;
        }}

        .metrics-card {{
            background-color: var(--bg-secondary);
            border-radius: 0.75rem;
            padding: 1.5rem;
            border: 1px solid var(--border-color);
            box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        }}

        .metrics-card h3 {{
            font-size: 1rem;
            font-weight: 600;
            margin-bottom: 1rem;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }}

        .metrics-card h3 .icon {{
            font-size: 1.25rem;
        }}

        /* Breakdown Bars */
        .breakdown-section {{
            margin-bottom: 1.5rem;
        }}

        .breakdown-title {{
            font-size: 0.8rem;
            font-weight: 600;
            color: var(--text-secondary);
            margin-bottom: 0.75rem;
        }}

        .breakdown-row {{
            display: flex;
            align-items: center;
            gap: 0.75rem;
            margin-bottom: 0.5rem;
        }}

        .breakdown-label {{
            font-size: 0.75rem;
            font-weight: 500;
            width: 60px;
            color: var(--text-secondary);
        }}

        .breakdown-bar-container {{
            flex: 1;
            height: 8px;
            background-color: var(--bg-card);
            border-radius: 4px;
            overflow: hidden;
        }}

        .breakdown-bar {{
            height: 100%;
            border-radius: 4px;
            transition: width 0.3s ease;
        }}

        .breakdown-value {{
            font-size: 0.75rem;
            font-weight: 600;
            width: 70px;
            text-align: right;
            color: var(--text-primary);
        }}

        /* Chart Container - taller for horizontal bars */
        .chart-container {{
            height: 300px;
            position: relative;
        }}

        .chart-summary {{
            display: flex;
            justify-content: space-between;
            margin-top: 0.75rem;
            padding-top: 0.5rem;
            border-top: 1px solid var(--border-color);
        }}

        .summary-item {{
            font-size: 0.7rem;
            padding: 0.35rem 0.6rem;
            border-radius: 0.25rem;
            background-color: var(--bg-card);
        }}

        .summary-label {{
            color: var(--text-muted);
            margin-right: 0.25rem;
        }}

        .summary-item.fastest {{
            color: var(--accent-green);
            border-left: 3px solid var(--accent-green);
        }}

        .summary-item.slowest {{
            color: var(--accent-red);
            border-left: 3px solid var(--accent-red);
        }}

        .summary-item.lowest {{
            color: var(--accent-red);
            border-left: 3px solid var(--accent-red);
        }}

        .summary-item.highest {{
            color: var(--accent-green);
            border-left: 3px solid var(--accent-green);
        }}

        /* Coverage Section */
        .coverage-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 1rem;
        }}

        .coverage-item {{
            text-align: center;
            padding: 1.25rem 1rem;
            background-color: var(--bg-card);
            border-radius: 0.5rem;
            border: 1px solid var(--border-color);
        }}

        .coverage-label {{
            font-size: 0.75rem;
            color: var(--text-secondary);
            margin-bottom: 0.5rem;
        }}

        .coverage-value {{
            font-size: 1.5rem;
            font-weight: 700;
            color: var(--accent-blue);
        }}

        .coverage-detail {{
            font-size: 0.7rem;
            color: var(--text-muted);
            margin-top: 0.25rem;
        }}

        /* Data Ingestion Score (DIS) */
        .dis-card .value {{
            font-size: 2.25rem;
        }}

        /* Trend indicators */
        .trend-up {{
            color: var(--accent-green);
            font-size: 0.85rem;
            font-weight: 600;
        }}

        .trend-down {{
            color: var(--accent-red);
            font-size: 0.85rem;
            font-weight: 600;
        }}

        .trend-neutral {{
            color: var(--text-muted);
            font-size: 0.85rem;
        }}

        .trend-na {{
            color: var(--text-muted);
            font-size: 0.8rem;
        }}

        .dis-card .trend-up,
        .dis-card .trend-down,
        .dis-card .trend-neutral {{
            font-size: 1rem;
            margin-left: 0.5rem;
        }}

        .dis-sublabel {{
            font-size: 0.65rem;
            color: #64748b;
            margin-top: 0.25rem;
        }}

        .dis-overview {{
            margin-bottom: 1rem;
            padding: 0.75rem;
            background-color: var(--bg-card);
            border-radius: 0.5rem;
        }}

        .dis-formula {{
            font-size: 0.8rem;
            color: var(--text-secondary);
        }}

        .dis-formula code {{
            background-color: var(--bg-secondary);
            padding: 0.25rem 0.5rem;
            border-radius: 0.25rem;
            font-family: 'Monaco', 'Menlo', monospace;
            font-size: 0.75rem;
            margin-left: 0.5rem;
        }}

        .formula-label {{
            font-weight: 600;
            color: var(--text-muted);
        }}

        .dis-table-section {{
            margin-top: 1rem;
        }}

        .dis-table {{
            width: 100%;
            font-size: 0.8rem;
            border-collapse: collapse;
        }}

        .dis-table th,
        .dis-table td {{
            padding: 0.6rem 0.75rem;
            text-align: left;
            border-bottom: 1px solid var(--border-color);
        }}

        .dis-table th {{
            color: var(--text-muted);
            font-weight: 500;
            text-transform: uppercase;
            font-size: 0.65rem;
            background-color: var(--bg-card);
        }}

        .dis-table td {{
            color: var(--text-secondary);
        }}

        .dis-table tbody tr:hover {{
            background-color: var(--bg-card);
        }}

        /* Agency Cards */
        .agency-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(420px, 1fr));
            gap: 1.5rem;
        }}

        .agency-card {{
            background-color: var(--bg-secondary);
            border-radius: 0.75rem;
            padding: 1.75rem;
            border: 1px solid var(--border-color);
            box-shadow: 0 2px 4px rgba(0,0,0,0.08);
            transition: box-shadow 0.2s ease, transform 0.2s ease;
        }}

        .agency-card:hover {{
            box-shadow: 0 4px 12px rgba(0,0,0,0.12);
            transform: translateY(-2px);
        }}

        .agency-header {{
            margin-bottom: 1.25rem;
            padding-bottom: 1rem;
            border-bottom: 1px solid var(--border-color);
        }}

        .agency-name {{
            font-size: 1.1rem;
            font-weight: 600;
            color: var(--text-primary);
            margin-bottom: 0.375rem;
        }}

        .agency-code {{
            font-size: 0.8rem;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }}

        .agency-stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(70px, 1fr));
            gap: 0.75rem;
            margin-bottom: 1.25rem;
        }}

        .agency-stat {{
            text-align: center;
            padding: 0.75rem 0.5rem;
            background-color: var(--bg-card);
            border-radius: 0.5rem;
        }}

        .agency-stat .stat-value {{
            font-size: 1.4rem;
            font-weight: 700;
            color: var(--text-primary);
        }}

        .agency-stat .stat-label {{
            font-size: 0.7rem;
            color: var(--text-muted);
            text-transform: uppercase;
            margin-top: 0.25rem;
        }}

        .agency-coverage-bars {{
            display: flex;
            flex-direction: column;
            gap: 0.5rem;
        }}

        .coverage-bar-row {{
            display: flex;
            align-items: center;
            gap: 0.75rem;
        }}

        .coverage-bar-label {{
            font-size: 0.75rem;
            color: var(--text-secondary);
            width: 70px;
        }}

        .coverage-bar-track {{
            flex: 1;
            height: 8px;
            background-color: var(--bg-card);
            border-radius: 4px;
            overflow: hidden;
        }}

        .coverage-bar-fill {{
            height: 100%;
            border-radius: 4px;
            transition: width 0.3s ease;
        }}

        .coverage-bar-fill.acquisition {{
            background-color: var(--accent-blue);
        }}

        .coverage-bar-fill.parse {{
            background-color: var(--accent-purple);
        }}

        .coverage-bar-value {{
            font-size: 0.75rem;
            font-weight: 500;
            color: var(--text-secondary);
            width: 40px;
            text-align: right;
        }}

        /* Filter Bar */
        .filter-bar {{
            display: flex;
            align-items: center;
            flex-wrap: wrap;
            gap: 0.75rem;
            margin-bottom: 1.5rem;
            padding: 1rem;
            background-color: var(--bg-secondary);
            border-radius: 0.5rem;
            border: 1px solid var(--border-color);
        }}

        .filter-label {{
            font-size: 0.8rem;
            font-weight: 500;
            color: var(--text-secondary);
            margin-left: 0.5rem;
        }}

        .filter-label:first-child {{
            margin-left: 0;
        }}

        .filter-select {{
            padding: 0.5rem 1rem;
            border: 1px solid var(--border-color);
            border-radius: 0.375rem;
            font-size: 0.875rem;
            background-color: white;
            color: var(--text-primary);
            cursor: pointer;
        }}

        .filter-select:focus {{
            outline: none;
            border-color: var(--accent-blue);
        }}

        /* Badge Styles */
        .badge {{
            display: inline-block;
            padding: 0.2rem 0.6rem;
            border-radius: 9999px;
            font-size: 0.65rem;
            font-weight: 600;
            color: white;
            text-transform: uppercase;
            white-space: nowrap;
        }}

        .method-badge {{
            font-size: 0.65rem;
            color: var(--accent-purple);
            background-color: rgba(124, 58, 237, 0.1);
            padding: 0.15rem 0.4rem;
            border-radius: 0.25rem;
            font-weight: 600;
            font-family: monospace;
        }}

        .format-badge {{
            font-size: 0.65rem;
            color: #0891b2;
            background-color: rgba(8, 145, 178, 0.1);
            padding: 0.15rem 0.4rem;
            border-radius: 0.25rem;
            font-weight: 600;
            font-family: monospace;
        }}

        .parser-badge {{
            font-size: 0.65rem;
            color: var(--accent-yellow);
            background-color: rgba(202, 138, 4, 0.1);
            padding: 0.15rem 0.4rem;
            border-radius: 0.25rem;
            font-weight: 600;
            font-family: monospace;
        }}

        .duration-badge {{
            font-size: 0.7rem;
            color: var(--accent-blue);
            background-color: rgba(37, 99, 235, 0.1);
            padding: 0.2rem 0.5rem;
            border-radius: 0.25rem;
            font-weight: 500;
        }}

        .latest-step-badge {{
            font-size: 0.65rem;
            color: white;
            padding: 0.2rem 0.5rem;
            border-radius: 0.25rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.03em;
        }}

        .storage-link {{
            color: var(--accent-blue);
            text-decoration: none;
            word-break: break-all;
        }}

        .storage-link:hover {{
            text-decoration: underline;
        }}

        /* Workflow List */
        .workflows-list {{
            display: flex;
            flex-direction: column;
            gap: 0.5rem;
        }}

        .workflow-item {{
            background-color: var(--bg-secondary);
            border-radius: 0.5rem;
            border: 1px solid var(--border-color);
            overflow: hidden;
        }}

        .workflow-item.hidden {{
            display: none;
        }}

        .workflow-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 1rem 1.25rem;
            cursor: pointer;
            transition: background-color 0.15s ease;
        }}

        .workflow-header:hover {{
            background-color: var(--bg-card);
        }}

        .workflow-summary {{
            display: flex;
            align-items: center;
            gap: 1rem;
        }}

        .expand-icon {{
            font-family: monospace;
            font-size: 1.25rem;
            font-weight: bold;
            color: var(--text-muted);
            width: 1.5rem;
            text-align: center;
        }}

        .workflow-info {{
            display: flex;
            flex-direction: column;
            gap: 0.125rem;
        }}

        .dataset-name {{
            font-weight: 600;
            font-size: 0.95rem;
            color: var(--text-primary);
        }}

        .agency-name {{
            font-size: 0.75rem;
            color: var(--text-secondary);
        }}

        .workflow-meta {{
            display: flex;
            align-items: center;
            gap: 0.75rem;
        }}

        .last-processed {{
            font-size: 0.75rem;
            color: var(--text-muted);
        }}

        .workflow-details {{
            display: none;
            padding: 0 1.25rem 1.25rem;
            border-top: 1px solid var(--border-color);
            background-color: var(--bg-card);
        }}

        .workflow-details.expanded {{
            display: block;
        }}

        .steps-container {{
            display: flex;
            flex-direction: column;
            gap: 1rem;
            padding-top: 1rem;
        }}

        .step-card {{
            background-color: var(--bg-secondary);
            border-radius: 0.5rem;
            padding: 1rem;
            border: 1px solid var(--border-color);
        }}

        .step-card.collapsible .step-header {{
            cursor: pointer;
            user-select: none;
        }}

        .step-card.collapsible .step-header:hover {{
            background-color: var(--bg-hover);
            margin: -0.5rem -0.5rem 0 -0.5rem;
            padding: 0.5rem;
            border-radius: 0.25rem;
        }}

        .step-card.collapsed .step-details {{
            display: none;
        }}

        .step-card.collapsed .step-header {{
            margin-bottom: 0;
        }}

        .step-header {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 0.5rem;
            margin-bottom: 0.75rem;
        }}

        .step-header-left {{
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }}

        .step-toggle-icon {{
            font-size: 0.7rem;
            color: var(--text-muted);
            transition: transform 0.2s ease;
            width: 0.8rem;
        }}

        .step-card:not(.collapsed) .step-toggle-icon {{
            transform: rotate(90deg);
        }}

        .step-name {{
            font-weight: 600;
            font-size: 0.9rem;
        }}

        .step-type {{
            color: var(--text-muted);
            font-size: 0.75rem;
        }}

        .step-details {{
            display: flex;
            flex-direction: column;
            gap: 0.4rem;
        }}

        .step-detail-row {{
            display: flex;
            gap: 0.75rem;
            font-size: 0.8rem;
        }}

        .detail-label {{
            color: var(--text-muted);
            min-width: 120px;
        }}

        .detail-value {{
            color: var(--text-secondary);
        }}

        .detail-value.monospace {{
            font-family: 'Monaco', 'Menlo', monospace;
            font-size: 0.7rem;
            word-break: break-all;
        }}

        /* Quality Section */
        .quality-section {{
            margin-top: 1rem;
            padding-top: 1rem;
            border-top: 1px solid var(--border-color);
        }}

        .quality-header {{
            font-size: 0.8rem;
            font-weight: 600;
            color: var(--text-secondary);
            margin-bottom: 0.75rem;
            display: flex;
            align-items: center;
        }}

        .quality-scores {{
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 0.5rem;
            margin-bottom: 0.75rem;
        }}

        .score-item {{
            text-align: center;
            background-color: var(--bg-card);
            padding: 0.5rem;
            border-radius: 0.375rem;
        }}

        .score-label {{
            display: block;
            font-size: 0.6rem;
            color: var(--text-muted);
            text-transform: uppercase;
        }}

        .score-value {{
            display: block;
            font-size: 1rem;
            font-weight: 700;
            color: var(--accent-green);
        }}

        .quality-metrics {{
            display: flex;
            gap: 0.5rem;
            flex-wrap: wrap;
        }}

        .metric {{
            font-size: 0.7rem;
            color: var(--text-secondary);
            background-color: var(--bg-card);
            padding: 0.2rem 0.4rem;
            border-radius: 0.25rem;
        }}

        .doc-type-badge {{
            display: inline-block;
            padding: 0.1rem 0.4rem;
            border-radius: 0.25rem;
            font-size: 0.6rem;
            font-weight: 600;
            color: white;
            text-transform: uppercase;
            margin-left: 0.5rem;
            vertical-align: middle;
        }}

        /* Text metrics */
        .text-metrics-section {{
            margin-top: 0.75rem;
            padding-top: 0.75rem;
            border-top: 1px solid var(--border-color);
        }}

        .text-metrics-header {{
            font-size: 0.75rem;
            font-weight: 600;
            color: var(--text-muted);
            margin-bottom: 0.5rem;
        }}

        .text-metrics-grid {{
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 0.4rem;
            margin-bottom: 0.5rem;
        }}

        .text-metric-item {{
            background-color: var(--bg-card);
            padding: 0.35rem 0.5rem;
            border-radius: 0.25rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}

        .text-metric-label {{
            font-size: 0.65rem;
            color: var(--text-muted);
        }}

        .text-metric-value {{
            font-size: 0.75rem;
            font-weight: 600;
            color: var(--text-secondary);
        }}

        .text-scores {{
            display: flex;
            gap: 0.4rem;
            flex-wrap: wrap;
        }}

        /* Tables section */
        .tables-section {{
            margin-top: 0.75rem;
        }}

        .tables-header {{
            font-size: 0.75rem;
            font-weight: 600;
            color: var(--text-muted);
            margin-bottom: 0.5rem;
        }}

        .tables-detail {{
            width: 100%;
            font-size: 0.7rem;
            border-collapse: collapse;
        }}

        .tables-detail th,
        .tables-detail td {{
            padding: 0.4rem;
            text-align: left;
            border-bottom: 1px solid var(--border-color);
        }}

        .tables-detail th {{
            color: var(--text-muted);
            font-weight: 500;
            text-transform: uppercase;
            font-size: 0.6rem;
        }}

        .tables-detail td {{
            color: var(--text-secondary);
        }}

        /* Enrichment Quality Section */
        .enrichment-quality {{
            background-color: rgba(22, 163, 74, 0.05);
            border: 1px solid rgba(22, 163, 74, 0.2);
            border-radius: 0.5rem;
            padding: 1rem;
        }}

        .relationship-quality {{
            background-color: rgba(139, 92, 246, 0.05);
            border: 1px solid rgba(139, 92, 246, 0.2);
            border-radius: 0.5rem;
            padding: 1rem;
            margin-top: 0.75rem;
        }}

        .relationship-quality h5 {{
            font-size: 0.85rem;
            font-weight: 600;
            color: var(--accent-purple);
            margin-bottom: 0.5rem;
        }}

        .relationship-metrics {{
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 0.5rem;
            margin-top: 0.5rem;
        }}

        .relationship-metric {{
            background-color: var(--bg-card);
            padding: 0.5rem;
            border-radius: 0.375rem;
        }}

        .relationship-metric-label {{
            font-size: 0.7rem;
            color: var(--text-muted);
            text-transform: uppercase;
        }}

        .relationship-metric-value {{
            font-size: 0.9rem;
            font-weight: 600;
            color: var(--text-primary);
        }}

        .enricher-badge {{
            display: inline-block;
            padding: 0.15rem 0.5rem;
            border-radius: 0.25rem;
            font-size: 0.7rem;
            font-weight: 600;
            color: white;
            background-color: var(--accent-blue);
            text-transform: uppercase;
        }}

        .enrichment-summary {{
            margin-top: 0.75rem;
            padding: 0.75rem;
            background-color: var(--bg-card);
            border-radius: 0.375rem;
            font-size: 0.8rem;
            line-height: 1.4;
        }}

        .summary-label {{
            font-weight: 600;
            color: var(--text-secondary);
            margin-right: 0.5rem;
        }}

        .summary-text {{
            color: var(--text-muted);
        }}

        .entities-section,
        .topics-section {{
            margin-top: 0.75rem;
        }}

        .entities-header,
        .topics-header {{
            font-size: 0.75rem;
            font-weight: 600;
            color: var(--text-muted);
            margin-bottom: 0.5rem;
        }}

        .entity-tags,
        .topic-tags {{
            display: flex;
            flex-wrap: wrap;
            gap: 0.4rem;
        }}

        .entity-tag {{
            display: inline-flex;
            align-items: center;
            gap: 0.25rem;
            padding: 0.2rem 0.5rem;
            border-radius: 0.25rem;
            font-size: 0.7rem;
            background-color: var(--bg-card);
            color: var(--text-secondary);
            border: 1px solid var(--border-color);
        }}

        .entity-tag small {{
            color: var(--text-muted);
            font-size: 0.6rem;
        }}

        .entity-organization {{ background-color: #dbeafe; border-color: #93c5fd; }}
        .entity-agency {{ background-color: #fef3c7; border-color: #fcd34d; }}
        .entity-form {{ background-color: #d1fae5; border-color: #6ee7b7; }}
        .entity-money {{ background-color: #e0e7ff; border-color: #a5b4fc; }}
        .entity-date {{ background-color: #fce7f3; border-color: #f9a8d4; }}
        .entity-percentage {{ background-color: #cffafe; border-color: #67e8f9; }}

        .topic-tag {{
            display: inline-block;
            padding: 0.2rem 0.5rem;
            border-radius: 0.25rem;
            font-size: 0.7rem;
            background-color: #e0e7ff;
            color: #4338ca;
            border: 1px solid #a5b4fc;
        }}

        .coverage-bar-fill.onboarding {{
            background-color: #8b5cf6;
        }}

        .coverage-bar-fill.enrichment {{
            background-color: var(--accent-green);
        }}

        /* Onboarding Step Styles */
        .onboarding-info {{
            background-color: rgba(139, 92, 246, 0.05);
            border: 1px solid rgba(139, 92, 246, 0.2);
            border-radius: 0.5rem;
            padding: 1rem;
        }}

        .onboarding-description {{
            font-size: 0.85rem;
            color: var(--text-secondary);
            line-height: 1.5;
        }}

        .source-link {{
            color: var(--accent-blue);
            text-decoration: none;
            font-size: 0.8rem;
            word-break: break-all;
        }}

        .source-link:hover {{
            text-decoration: underline;
        }}

        .schedule-badge {{
            display: inline-block;
            padding: 0.15rem 0.5rem;
            border-radius: 0.25rem;
            font-size: 0.7rem;
            font-weight: 600;
            color: white;
            background-color: #8b5cf6;
            text-transform: uppercase;
        }}

        .labels-row {{
            flex-wrap: wrap;
        }}

        .labels-container {{
            display: flex;
            flex-wrap: wrap;
            gap: 0.4rem;
        }}

        .label-tag {{
            display: inline-block;
            padding: 0.2rem 0.5rem;
            border-radius: 0.25rem;
            font-size: 0.7rem;
            background-color: var(--bg-card);
            color: var(--text-secondary);
            border: 1px solid var(--border-color);
        }}

        /* Document Type Distribution */
        .doc-type-distribution {{
            margin-top: 1rem;
        }}

        .doc-type-badges {{
            display: flex;
            flex-wrap: wrap;
            gap: 0.75rem;
            margin-top: 0.5rem;
        }}

        .doc-type-item {{
            display: flex;
            align-items: center;
            gap: 0.5rem;
            padding: 0.5rem 0.75rem;
            background-color: var(--bg-card);
            border-radius: 0.375rem;
            border: 1px solid var(--border-color);
        }}

        .doc-type-item .doc-type-badge {{
            padding: 0.2rem 0.5rem;
            font-size: 0.7rem;
        }}

        .doc-type-count {{
            font-size: 0.8rem;
            font-weight: 600;
            color: var(--text-secondary);
        }}

        /* Quality Breakdown Section */
        .quality-breakdown {{
            margin-top: 1rem;
            padding: 1rem;
            background-color: var(--bg-card);
            border-radius: 0.5rem;
            border: 1px solid var(--border-color);
        }}

        .quality-breakdown-title {{
            font-size: 0.8rem;
            font-weight: 600;
            color: var(--text-secondary);
            margin-bottom: 0.75rem;
        }}

        .quality-breakdown-grid {{
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 1rem;
        }}

        .quality-breakdown-item {{
            text-align: center;
            padding: 0.5rem;
            background-color: var(--bg-secondary);
            border-radius: 0.375rem;
        }}

        .quality-breakdown-item.composite {{
            background-color: rgba(37, 99, 235, 0.1);
            border: 1px solid rgba(37, 99, 235, 0.3);
        }}

        .quality-breakdown-item .breakdown-label {{
            display: block;
            font-size: 0.7rem;
            color: var(--text-muted);
            margin-bottom: 0.25rem;
        }}

        .quality-breakdown-item .breakdown-score {{
            display: block;
            font-size: 1rem;
            font-weight: 700;
        }}

        /* Entity Relationships Page Styles */
        .graph-controls {{
            display: flex;
            align-items: center;
            flex-wrap: wrap;
            gap: 1rem;
            padding: 1rem;
            background-color: var(--bg-secondary);
            border-radius: 0.5rem;
            border: 1px solid var(--border-color);
            margin-bottom: 1rem;
        }}

        .graph-controls label {{
            font-size: 0.8rem;
            color: var(--text-secondary);
            margin-right: 0.5rem;
        }}

        .graph-controls select,
        .graph-controls input {{
            padding: 0.4rem 0.75rem;
            border: 1px solid var(--border-color);
            border-radius: 0.375rem;
            font-size: 0.8rem;
            background-color: white;
            color: var(--text-primary);
        }}

        .graph-controls input[type="range"] {{
            width: 100px;
        }}

        .graph-controls .control-group {{
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }}

        #entity-network-container {{
            height: 500px;
            border: 1px solid var(--border-color);
            border-radius: 0.5rem;
            background-color: var(--bg-secondary);
            position: relative;
        }}

        .network-legend {{
            position: absolute;
            bottom: 10px;
            left: 10px;
            background-color: rgba(255, 255, 255, 0.95);
            padding: 0.75rem;
            border-radius: 0.5rem;
            border: 1px solid var(--border-color);
            font-size: 0.7rem;
            z-index: 10;
        }}

        .network-legend-item {{
            display: flex;
            align-items: center;
            gap: 0.5rem;
            margin-bottom: 0.25rem;
        }}

        .network-legend-item:last-child {{
            margin-bottom: 0;
        }}

        .legend-dot {{
            width: 12px;
            height: 12px;
            border-radius: 50%;
        }}

        .cluster-card {{
            background-color: var(--bg-secondary);
            border-radius: 0.5rem;
            padding: 1rem;
            border: 1px solid var(--border-color);
            margin-bottom: 0.75rem;
            transition: box-shadow 0.15s ease;
        }}

        .cluster-card:hover {{
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
        }}

        .cluster-header {{
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            margin-bottom: 0.5rem;
        }}

        .cluster-name {{
            font-weight: 600;
            font-size: 0.9rem;
            color: var(--text-primary);
        }}

        .cluster-doc-count {{
            font-size: 0.75rem;
            color: var(--text-muted);
            background-color: var(--bg-card);
            padding: 0.2rem 0.5rem;
            border-radius: 0.25rem;
        }}

        .cluster-entities {{
            display: flex;
            flex-wrap: wrap;
            gap: 0.3rem;
            margin-bottom: 0.5rem;
        }}

        .cluster-entity-tag {{
            display: inline-block;
            padding: 0.15rem 0.4rem;
            border-radius: 0.25rem;
            font-size: 0.65rem;
            background-color: var(--bg-card);
            color: var(--text-secondary);
            border: 1px solid var(--border-color);
        }}

        .cluster-overlap {{
            font-size: 0.7rem;
            color: var(--text-muted);
        }}

        .cluster-docs {{
            font-size: 0.7rem;
            color: var(--text-secondary);
            margin-top: 0.5rem;
            padding-top: 0.5rem;
            border-top: 1px solid var(--border-color);
        }}

        .entity-details-panel {{
            position: fixed;
            right: -400px;
            top: 0;
            width: 380px;
            height: 100vh;
            background-color: var(--bg-secondary);
            border-left: 1px solid var(--border-color);
            box-shadow: -4px 0 12px rgba(0, 0, 0, 0.1);
            z-index: 1000;
            transition: right 0.3s ease;
            overflow-y: auto;
            padding: 1.5rem;
        }}

        .entity-details-panel.visible {{
            right: 0;
        }}

        .entity-details-close {{
            position: absolute;
            top: 1rem;
            right: 1rem;
            background: none;
            border: none;
            font-size: 1.5rem;
            cursor: pointer;
            color: var(--text-muted);
        }}

        .entity-details-close:hover {{
            color: var(--text-primary);
        }}

        .entity-details-title {{
            font-size: 1.1rem;
            font-weight: 600;
            color: var(--text-primary);
            margin-bottom: 0.25rem;
            padding-right: 2rem;
        }}

        .entity-details-type {{
            display: inline-block;
            padding: 0.2rem 0.5rem;
            border-radius: 0.25rem;
            font-size: 0.7rem;
            font-weight: 600;
            text-transform: uppercase;
            margin-bottom: 1rem;
        }}

        .entity-details-section {{
            margin-bottom: 1.25rem;
        }}

        .entity-details-section-title {{
            font-size: 0.75rem;
            font-weight: 600;
            color: var(--text-muted);
            text-transform: uppercase;
            margin-bottom: 0.5rem;
        }}

        .entity-details-stat {{
            display: flex;
            justify-content: space-between;
            padding: 0.4rem 0;
            border-bottom: 1px solid var(--border-color);
            font-size: 0.85rem;
        }}

        .entity-details-stat:last-child {{
            border-bottom: none;
        }}

        .entity-details-doc {{
            display: block;
            padding: 0.3rem 0.5rem;
            margin-bottom: 0.25rem;
            background-color: var(--bg-card);
            border-radius: 0.25rem;
            font-size: 0.8rem;
            color: var(--text-secondary);
        }}

        .entity-type-geography {{ background-color: #dbeafe; color: #1e40af; }}
        .entity-type-agency {{ background-color: #fef3c7; color: #92400e; }}
        .entity-type-program {{ background-color: #d1fae5; color: #065f46; }}
        .entity-type-organization {{ background-color: #e0e7ff; color: #3730a3; }}
        .entity-type-form {{ background-color: #fce7f3; color: #9d174d; }}
        .entity-type-money {{ background-color: #cffafe; color: #0e7490; }}
        .entity-type-date {{ background-color: #fef9c3; color: #854d0e; }}
        .entity-type-other {{ background-color: #f3f4f6; color: #374151; }}

        .charts-row {{
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 1.5rem;
            margin-bottom: 1.5rem;
        }}

        .entity-chart-container {{
            height: 280px;
        }}

        .clusters-section {{
            margin-top: 1.5rem;
        }}

        .clusters-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(350px, 1fr));
            gap: 1rem;
        }}

        /* Footer */
        footer {{
            margin-top: 3rem;
            padding-top: 1rem;
            border-top: 1px solid var(--border-color);
            text-align: center;
            color: var(--text-muted);
            font-size: 0.8rem;
        }}

        footer a {{
            color: var(--accent-blue);
            text-decoration: none;
        }}

        footer a:hover {{
            text-decoration: underline;
        }}

        @media (max-width: 1024px) {{
            .metrics-row {{
                grid-template-columns: 1fr;
            }}
        }}

        @media (max-width: 768px) {{
            .sidebar {{
                display: none;
            }}

            .main-content {{
                margin-left: 0;
            }}

            .workflow-header {{
                flex-direction: column;
                align-items: flex-start;
                gap: 0.75rem;
            }}

            .workflow-meta {{
                margin-left: 2.5rem;
            }}

            .quality-scores {{
                grid-template-columns: repeat(2, 1fr);
            }}

            .text-metrics-grid {{
                grid-template-columns: repeat(2, 1fr);
            }}

            .step-detail-row {{
                flex-direction: column;
                gap: 0.2rem;
            }}

            .detail-label {{
                min-width: auto;
            }}
        }}
    </style>
</head>
<body>
    <!-- Sidebar Navigation -->
    <nav class="sidebar">
        <div class="sidebar-logo">Pipeline Report</div>
        <div class="sidebar-subtitle">Generated: {generated_at.strftime("%b %d, %Y %H:%M")}</div>

        <div class="nav-section">
            <div class="nav-section-title">Pages</div>
            <a class="nav-item active" onclick="showPage('dashboard')" id="nav-dashboard">
                {icon_dashboard}
                Executive Dashboard
            </a>
            <a class="nav-item" onclick="showPage('agencies')" id="nav-agencies">
                {icon_building}
                Agencies
            </a>
            <a class="nav-item" onclick="showPage('assets')" id="nav-assets">
                {icon_folder}
                Assets
            </a>
            <a class="nav-item" onclick="showPage('entities')" id="nav-entities">
                {icon_network}
                Entity Relationships
            </a>
        </div>

        <div class="nav-section">
            <div class="nav-section-title">Quick Stats</div>
            <div style="padding: 0.5rem 1rem; font-size: 0.8rem;">
                <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                    <span style="color: var(--text-muted);">DIS Score</span>
                    <span style="font-weight: 600;" class="{dis_color}">{overall_dis_str} {overall_dis_trend_html}</span>
                </div>
                <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                    <span style="color: var(--text-muted);">Total Assets</span>
                    <span style="font-weight: 600;">{metrics.total_assets}</span>
                </div>
                <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                    <span style="color: var(--text-muted);">Onboarded</span>
                    <span style="font-weight: 600;">{metrics.total_workflows}</span>
                </div>
                <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                    <span style="color: var(--text-muted);">Executed</span>
                    <span style="font-weight: 600; color: var(--accent-blue);">{metrics.executed_workflows}</span>
                </div>
                <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                    <span style="color: var(--text-muted);">Successful</span>
                    <span style="font-weight: 600; color: var(--accent-green);">{metrics.successful_workflows}</span>
                </div>
                <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                    <span style="color: var(--text-muted);">Success Rate</span>
                    <span style="font-weight: 600; color: var(--accent-green);">{(metrics.successful_workflows / metrics.executed_workflows * 100) if metrics.executed_workflows > 0 else 0:.0f}%</span>
                </div>
                <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                    <span style="color: var(--text-muted);">Eligible Coverage</span>
                    <span style="font-weight: 600; color: var(--accent-purple);">{(metrics.successful_workflows / metrics.total_workflows * 100) if metrics.total_workflows > 0 else 0:.0f}%</span>
                </div>
                <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                    <span style="color: var(--text-muted);">Avg Duration</span>
                    <span style="font-weight: 600; color: var(--accent-yellow);">{avg_duration_str}</span>
                </div>
                <div style="display: flex; justify-content: space-between;">
                    <span style="color: var(--text-muted);">Avg Quality</span>
                    <span style="font-weight: 600; color: var(--accent-green);">{avg_quality_str}</span>
                </div>
            </div>
        </div>
    </nav>

    <!-- Main Content -->
    <main class="main-content">
        <!-- Page 1: Executive Dashboard -->
        <div class="page active" id="page-dashboard">
            <div class="page-header">
                <h1>Executive Dashboard</h1>
                <p>Overview of pipeline performance, quality metrics, and coverage statistics</p>
            </div>

            <!-- Summary Cards -->
            <div class="summary-grid">
                <div class="summary-card dis-card">
                    <h4>Data Ingestion Score</h4>
                    <div class="value {dis_color}">{overall_dis_str} {overall_dis_trend_html}</div>
                    <div class="dis-sublabel">Quality + Efficiency + Exec Success</div>
                </div>
                <div class="summary-card">
                    <h4>Total Assets</h4>
                    <div class="value blue">{metrics.total_assets}</div>
                </div>
                <div class="summary-card">
                    <h4>Workflows Executed</h4>
                    <div class="value purple">{metrics.executed_workflows}</div>
                    <div class="sublabel">{metrics.total_workflows} onboarded</div>
                </div>
                <div class="summary-card">
                    <h4>Successful</h4>
                    <div class="value green">{metrics.successful_workflows}</div>
                    <div class="sublabel">{(metrics.successful_workflows / metrics.executed_workflows * 100) if metrics.executed_workflows > 0 else 0:.0f}% success rate</div>
                </div>
                <div class="summary-card">
                    <h4>Eligible Coverage</h4>
                    <div class="value purple">{(metrics.successful_workflows / metrics.total_workflows * 100) if metrics.total_workflows > 0 else 0:.0f}%</div>
                    <div class="sublabel">{metrics.successful_workflows} of {metrics.total_workflows} onboarded</div>
                </div>
                <div class="summary-card">
                    <h4>Avg Duration</h4>
                    <div class="value yellow">{avg_duration_str}</div>
                </div>
                <div class="summary-card">
                    <h4>Avg Quality</h4>
                    <div class="value green">{avg_quality_str}</div>
                </div>
            </div>

            <!-- Time and Quality Metrics -->
            <div class="metrics-row">
                <!-- Time Metrics -->
                <div class="metrics-card">
                    <h3>{icon_clock} Processing Time</h3>

                    <div class="breakdown-section">
                        <div class="breakdown-title">Average by File Type</div>
                        {duration_bars_html if duration_bars_html else '<p style="color: var(--text-muted); font-size: 0.8rem;">No timing data available</p>'}
                    </div>

                    <div class="breakdown-title">Distribution</div>
                    <div class="chart-container">
                        <canvas id="durationChart"></canvas>
                    </div>
                    <div class="chart-summary">
                        <span class="summary-item fastest"><span class="summary-label">Fastest:</span> {min_duration_asset}</span>
                        <span class="summary-item slowest"><span class="summary-label">Slowest:</span> {max_duration_asset}</span>
                    </div>
                </div>

                <!-- Quality Metrics -->
                <div class="metrics-card">
                    <h3>{icon_star} Quality Scores</h3>

                    <div class="breakdown-section">
                        <div class="breakdown-title">Average by File Type</div>
                        {quality_bars_html if quality_bars_html else '<p style="color: var(--text-muted); font-size: 0.8rem;">No quality data available</p>'}
                    </div>

                    <div class="breakdown-title">Distribution by Asset (Composite: Parse×0.5 + Enrich×0.25 + Rel×0.25)</div>
                    <div class="chart-container">
                        <canvas id="qualityChart"></canvas>
                    </div>
                    <div class="chart-summary">
                        <span class="summary-item lowest"><span class="summary-label">Lowest:</span> {min_quality_asset}</span>
                        <span class="summary-item highest"><span class="summary-label">Highest:</span> {max_quality_asset}</span>
                    </div>
                </div>
            </div>

            <!-- Coverage Metrics -->
            <div class="metrics-card">
                <h3>{icon_chart} Step Coverage</h3>
                <p style="color: var(--text-secondary); font-size: 0.8rem; margin-bottom: 1rem;">
                    Percentage of registered assets that have completed each pipeline step
                </p>
                <div class="coverage-grid">
                    <div class="coverage-item">
                        <div class="coverage-label">Acquisition Step</div>
                        <div class="coverage-value">{metrics.acquisition_coverage:.0f}%</div>
                        <div class="coverage-detail">{metrics.assets_with_acquisition} of {metrics.total_assets} assets</div>
                    </div>
                    <div class="coverage-item">
                        <div class="coverage-label">Parse Step</div>
                        <div class="coverage-value">{metrics.parse_coverage:.0f}%</div>
                        <div class="coverage-detail">{metrics.assets_with_parse} of {metrics.total_assets} assets</div>
                    </div>
                    <div class="coverage-item">
                        <div class="coverage-label">Enrichment Step</div>
                        <div class="coverage-value">{metrics.enrichment_coverage:.0f}%</div>
                        <div class="coverage-detail">{metrics.assets_with_enrichment} of {metrics.total_assets} assets</div>
                    </div>
                </div>
            </div>

            <!-- Parse Metrics Card -->
            <div class="metrics-card">
                <h3>{icon_star} Quality Metrics Parser</h3>
                <p style="color: var(--text-secondary); font-size: 0.8rem; margin-bottom: 1rem;">
                    Document parsing metrics including structure extraction and content analysis
                </p>
                <div class="coverage-grid">
                    <div class="coverage-item">
                        <div class="coverage-label">Total Tables Extracted</div>
                        <div class="coverage-value">{metrics.total_tables_extracted:,}</div>
                        <div class="coverage-detail">Across all parsed documents</div>
                    </div>
                    <div class="coverage-item">
                        <div class="coverage-label">Total Sections</div>
                        <div class="coverage-value">{metrics.total_sections_extracted:,}</div>
                        <div class="coverage-detail">Document structure elements</div>
                    </div>
                    <div class="coverage-item">
                        <div class="coverage-label">Total Tokens</div>
                        <div class="coverage-value">{metrics.total_tokens_extracted:,}</div>
                        <div class="coverage-detail">Estimated LLM tokens</div>
                    </div>
                    <div class="coverage-item">
                        <div class="coverage-label">Avg Parse Quality</div>
                        <div class="coverage-value">{f'{metrics.avg_parse_quality:.1f}' if metrics.avg_parse_quality else 'N/A'}</div>
                        <div class="coverage-detail">Overall parse quality score</div>
                    </div>
                    <div class="coverage-item">
                        <div class="coverage-label">Estimated Cost</div>
                        <div class="coverage-value">${metrics.total_parse_cost:.2f}</div>
                        <div class="coverage-detail">Nanonets: 100 credits = $1</div>
                    </div>
                </div>
                {doc_type_distribution_html}
            </div>

            <!-- Enrichment Metrics Card -->
            <div class="metrics-card">
                <h3>{icon_star} Quality Metrics Enrichment</h3>
                <p style="color: var(--text-secondary); font-size: 0.8rem; margin-bottom: 1rem;">
                    RAG-readiness metrics for enriched documents (entity extraction, topic coverage, semantic context)
                </p>
                <div class="coverage-grid">
                    <div class="coverage-item">
                        <div class="coverage-label">Total Entities Extracted</div>
                        <div class="coverage-value">{metrics.total_entities_extracted}</div>
                        <div class="coverage-detail">Across all enriched documents</div>
                    </div>
                    <div class="coverage-item">
                        <div class="coverage-label">Total Topics Identified</div>
                        <div class="coverage-value">{metrics.total_topics_extracted}</div>
                        <div class="coverage-detail">Key themes and subjects</div>
                    </div>
                    <div class="coverage-item">
                        <div class="coverage-label">Estimated Cost</div>
                        <div class="coverage-value">${metrics.total_enrichment_cost:.4f}</div>
                        <div class="coverage-detail">LLM API costs (USD)</div>
                    </div>
                </div>
            </div>

            <!-- Data Ingestion Score (DIS) Section -->
            <div class="metrics-card">
                <h3>{icon_gauge} Data Ingestion Score (DIS)</h3>
                <p style="color: var(--text-secondary); font-size: 0.8rem; margin-bottom: 1rem;">
                    Composite metric combining Quality (40%), Efficiency (30%), and Execution Success (30%) per workflow
                </p>

                <div class="dis-overview">
                    <div class="dis-formula">
                        <span class="formula-label">Formula:</span>
                        <code>DIS = (Quality × 0.40) + (Efficiency × 0.30) + (Exec Success × 0.30)</code>
                    </div>
                </div>

                <div class="dis-table-section">
                    <div class="breakdown-title" style="margin-top: 1rem;">Per-Workflow Breakdown</div>
                    <table class="dis-table">
                        <thead>
                            <tr>
                                <th>Workflow</th>
                                <th>DIS Score</th>
                                <th>Trend</th>
                                <th title="Composite: Parse×0.5 + Enrich×0.25 + Rel×0.25">Quality (40%)</th>
                                <th>Efficiency (30%)</th>
                                <th>Exec Success (30%)</th>
                            </tr>
                        </thead>
                        <tbody>
                            {dis_table_rows}
                        </tbody>
                    </table>
                </div>
            </div>

            <footer>
                <p>Government Data Pipeline &mdash; Quality metrics based on
                <a href="https://github.com/opendatalab/OmniDocBench" target="_blank">OmniDocBench (CVPR 2025)</a></p>
            </footer>
        </div>

        <!-- Page 2: Agencies Catalog -->
        <div class="page" id="page-agencies">
            <div class="page-header">
                <h1>Agency Catalog</h1>
                <p>Coverage and metrics breakdown by government agency</p>
            </div>

            <div class="agency-grid">
                {agency_cards_html}
            </div>

            <footer>
                <p>Government Data Pipeline &mdash; Quality metrics based on
                <a href="https://github.com/opendatalab/OmniDocBench" target="_blank">OmniDocBench (CVPR 2025)</a></p>
            </footer>
        </div>

        <!-- Page 3: Assets List -->
        <div class="page" id="page-assets">
            <div class="page-header">
                <h1>Assets</h1>
                <p>Detailed view of all pipeline assets and their processing status</p>
            </div>

            <!-- Filter Bar -->
            <div class="filter-bar">
                <span class="filter-label">Agency:</span>
                <select class="filter-select" id="agencyFilter" onchange="filterWorkflows()">
                    {agency_options}
                </select>
                <span class="filter-label">Format:</span>
                <select class="filter-select" id="formatFilter" onchange="filterWorkflows()">
                    {format_options}
                </select>
                <span class="filter-label">Latest Step:</span>
                <select class="filter-select" id="stepFilter" onchange="filterWorkflows()">
                    {latest_step_options}
                </select>
            </div>

            <!-- Workflows List -->
            <div class="workflows-list">
                {''.join(workflow_items)}
            </div>

            <footer>
                <p>Government Data Pipeline &mdash; Quality metrics based on
                <a href="https://github.com/opendatalab/OmniDocBench" target="_blank">OmniDocBench (CVPR 2025)</a></p>
            </footer>
        </div>

        <!-- Page 4: Entity Relationships & Clusters -->
        <div class="page" id="page-entities">
            <div class="page-header">
                <h1>Entity Relationships & Clusters</h1>
                <p>Cross-document entity connections and document groupings</p>
            </div>

            <!-- Summary Cards -->
            <div class="summary-grid">
                <div class="summary-card">
                    <h4>Total Entities</h4>
                    <div class="value blue">{entity_data.total_raw_entities:,}</div>
                    <div class="sublabel">Across all documents</div>
                </div>
                <div class="summary-card">
                    <h4>Unique Entities</h4>
                    <div class="value purple">{entity_data.total_unique_entities:,}</div>
                    <div class="sublabel">From enrichment step</div>
                </div>
                <div class="summary-card">
                    <h4>Co-occurrences</h4>
                    <div class="value green">{len(entity_data.relationships):,}</div>
                    <div class="sublabel">Entity connections</div>
                </div>
                <div class="summary-card">
                    <h4>Clusters</h4>
                    <div class="value yellow">{len(entity_data.clusters)}</div>
                    <div class="sublabel">Cross-document groups</div>
                </div>
            </div>

            <!-- Quality Metrics Detail -->
            <div class="metrics-card" style="margin-bottom: 1.5rem;">
                <h3>{icon_star} Entity Graph Metrics</h3>
                <p style="color: var(--text-secondary); font-size: 0.8rem; margin-bottom: 1rem;">
                    Entity co-occurrence metrics computed from enriched documents
                </p>
                <div class="coverage-grid">
                    <div class="coverage-item">
                        <div class="coverage-label">Unique Entities</div>
                        <div class="coverage-value">{entity_data.total_unique_entities:,}</div>
                        <div class="coverage-detail">Deduplicated across documents</div>
                    </div>
                    <div class="coverage-item">
                        <div class="coverage-label">Co-occurrence Edges</div>
                        <div class="coverage-value">{len(entity_data.relationships):,}</div>
                        <div class="coverage-detail">Entity pair connections</div>
                    </div>
                    <div class="coverage-item">
                        <div class="coverage-label">Cross-Doc Clusters</div>
                        <div class="coverage-value">{len(entity_data.clusters)}</div>
                        <div class="coverage-detail">Documents with shared entities</div>
                    </div>
                </div>
            </div>

            <!-- Charts Row -->
            <div class="metrics-row">
                <!-- Entity Type Distribution -->
                <div class="metrics-card">
                    <h3>{icon_chart} Entity Type Distribution</h3>
                    <div class="entity-chart-container">
                        <canvas id="entityTypeChart"></canvas>
                    </div>
                </div>

                <!-- Central Entities -->
                <div class="metrics-card">
                    <h3>{icon_star} Central Entities (Top 10)</h3>
                    <p style="color: var(--text-secondary); font-size: 0.75rem; margin-bottom: 0.5rem;">
                        Entities with the most connections across documents
                    </p>
                    <div class="entity-chart-container">
                        <canvas id="centralEntitiesChart"></canvas>
                    </div>
                </div>
            </div>

            <!-- Entity Network Graph -->
            <div class="metrics-card">
                <h3>{icon_network} Entity Relationship Network</h3>
                <p style="color: var(--text-secondary); font-size: 0.8rem; margin-bottom: 1rem;">
                    Interactive network showing entity relationships. Nodes sized by document count, edges by co-occurrence strength.
                </p>

                <div class="graph-controls">
                    <div class="control-group">
                        <label for="entityTypeFilter">Type:</label>
                        <select id="entityTypeFilter" onchange="filterEntityNetwork()">
                            <option value="all">All Types</option>
                            <option value="geography">Geography</option>
                            <option value="agency">Agency</option>
                            <option value="program">Program</option>
                            <option value="organization">Organization</option>
                            <option value="form">Form</option>
                            <option value="other">Other</option>
                        </select>
                    </div>
                    <div class="control-group">
                        <label for="minConnections">Min Connections:</label>
                        <input type="range" id="minConnections" min="1" max="10" value="1" onchange="filterEntityNetwork()">
                        <span id="minConnectionsValue">1</span>
                    </div>
                    <div class="control-group">
                        <label>
                            <input type="checkbox" id="showLabels" checked onchange="toggleLabels()">
                            Show Labels
                        </label>
                    </div>
                </div>

                <div id="entity-network-container">
                    <div class="network-legend">
                        <div class="network-legend-item">
                            <span class="legend-dot" style="background-color: #3b82f6;"></span>
                            <span>Geography</span>
                        </div>
                        <div class="network-legend-item">
                            <span class="legend-dot" style="background-color: #f59e0b;"></span>
                            <span>Agency</span>
                        </div>
                        <div class="network-legend-item">
                            <span class="legend-dot" style="background-color: #10b981;"></span>
                            <span>Program</span>
                        </div>
                        <div class="network-legend-item">
                            <span class="legend-dot" style="background-color: #8b5cf6;"></span>
                            <span>Organization</span>
                        </div>
                        <div class="network-legend-item">
                            <span class="legend-dot" style="background-color: #ec4899;"></span>
                            <span>Form</span>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Document Clusters -->
            <div class="metrics-card clusters-section">
                <h3>{icon_folder} Document Clusters</h3>
                <p style="color: var(--text-secondary); font-size: 0.8rem; margin-bottom: 1rem;">
                    Documents grouped by shared entities (Jaccard similarity threshold: 30%)
                </p>

                <div class="clusters-grid">
                    {clusters_html if clusters_html else '<p style="color: var(--text-muted);">No clusters detected. Documents may have insufficient entity overlap.</p>'}
                </div>
            </div>

            <!-- Entity Details Panel (hidden by default) -->
            <div class="entity-details-panel" id="entityDetailsPanel">
                <button class="entity-details-close" onclick="hideEntityDetails()">&times;</button>
                <div id="entityDetailsContent">
                    <!-- Populated by JavaScript -->
                </div>
            </div>

            <footer>
                <p>Government Data Pipeline &mdash; Quality metrics based on
                <a href="https://github.com/opendatalab/OmniDocBench" target="_blank">OmniDocBench (CVPR 2025)</a></p>
            </footer>
        </div>
    </main>

    <script src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
    <script>
        // Entity data for vis.js network
        const entityNodes = {vis_nodes_data};
        const entityEdges = {vis_edges_data};
        let entityNetwork = null;
        let allNodes = null;
        let allEdges = null;

        // Page navigation
        function showPage(pageId) {{
            // Hide all pages
            document.querySelectorAll('.page').forEach(page => {{
                page.classList.remove('active');
            }});

            // Remove active from all nav items
            document.querySelectorAll('.nav-item').forEach(item => {{
                item.classList.remove('active');
            }});

            // Show selected page
            document.getElementById('page-' + pageId).classList.add('active');
            document.getElementById('nav-' + pageId).classList.add('active');
        }}

        // Workflow expand/collapse
        function toggleWorkflow(idx) {{
            const details = document.getElementById('details-' + idx);
            const icon = document.getElementById('icon-' + idx);

            if (details.classList.contains('expanded')) {{
                details.classList.remove('expanded');
                icon.textContent = '+';
            }} else {{
                details.classList.add('expanded');
                icon.textContent = '-';
            }}
        }}

        // Step card expand/collapse
        function toggleStepCard(header) {{
            const card = header.closest('.step-card');
            if (card.classList.contains('collapsed')) {{
                card.classList.remove('collapsed');
            }} else {{
                card.classList.add('collapsed');
            }}
        }}

        // Expand/collapse all step cards
        function toggleAllStepCards(workflowIdx, expand) {{
            const container = document.getElementById('details-' + workflowIdx);
            if (container) {{
                container.querySelectorAll('.step-card.collapsible').forEach(card => {{
                    if (expand) {{
                        card.classList.remove('collapsed');
                    }} else {{
                        card.classList.add('collapsed');
                    }}
                }});
            }}
        }}

        // Filter workflows by agency, format, and latest step
        function filterWorkflows() {{
            const agencyFilter = document.getElementById('agencyFilter').value;
            const formatFilter = document.getElementById('formatFilter').value;
            const stepFilter = document.getElementById('stepFilter').value;
            document.querySelectorAll('.workflow-item').forEach(item => {{
                const matchesAgency = agencyFilter === 'all' || item.dataset.agency === agencyFilter;
                const matchesFormat = formatFilter === 'all' || item.dataset.format === formatFilter;
                const matchesStep = stepFilter === 'all' || item.dataset.latestStep === stepFilter;
                if (matchesAgency && matchesFormat && matchesStep) {{
                    item.classList.remove('hidden');
                }} else {{
                    item.classList.add('hidden');
                }}
            }});
        }}

        // Initialize charts
        document.addEventListener('DOMContentLoaded', function() {{
            // Duration data with per-step breakdown
            const durationLabels = {duration_labels_data};
            const acquisitionTimes = {acquisition_times_data};
            const parseTimes = {parse_times_data};

            // Quality data
            const qualityLabels = {quality_labels_data};
            const qualityValues = {quality_values_data};
            const qualitySteps = {quality_steps_data};

            // Helper to truncate long asset names
            function truncateName(name, maxLen = 25) {{
                return name.length > maxLen ? name.substring(0, maxLen) + '...' : name;
            }}

            // Helper to format duration
            function formatDuration(ms) {{
                if (ms < 1000) return ms.toFixed(0) + 'ms';
                if (ms < 60000) return (ms / 1000).toFixed(2) + 's';
                return (ms / 60000).toFixed(1) + 'm';
            }}

            // Duration Distribution Chart - Horizontal Stacked Bars
            if (durationLabels.length > 0) {{
                const durationCtx = document.getElementById('durationChart').getContext('2d');

                new Chart(durationCtx, {{
                    type: 'bar',
                    data: {{
                        labels: durationLabels.map(truncateName),
                        datasets: [
                            {{
                                label: 'Acquisition',
                                data: acquisitionTimes,
                                backgroundColor: 'rgba(37, 99, 235, 0.7)',
                                borderColor: 'rgba(37, 99, 235, 1)',
                                borderWidth: 1
                            }},
                            {{
                                label: 'Parse',
                                data: parseTimes,
                                backgroundColor: 'rgba(124, 58, 237, 0.7)',
                                borderColor: 'rgba(124, 58, 237, 1)',
                                borderWidth: 1
                            }}
                        ]
                    }},
                    options: {{
                        indexAxis: 'y',
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {{
                            legend: {{
                                display: true,
                                position: 'bottom',
                                labels: {{
                                    boxWidth: 12,
                                    padding: 15,
                                    font: {{ size: 10 }}
                                }}
                            }},
                            tooltip: {{
                                callbacks: {{
                                    title: function(context) {{
                                        return durationLabels[context[0].dataIndex];
                                    }},
                                    label: function(context) {{
                                        return context.dataset.label + ': ' + formatDuration(context.raw);
                                    }},
                                    afterBody: function(context) {{
                                        const idx = context[0].dataIndex;
                                        const total = acquisitionTimes[idx] + parseTimes[idx];
                                        return 'Total: ' + formatDuration(total);
                                    }}
                                }}
                            }}
                        }},
                        scales: {{
                            x: {{
                                stacked: true,
                                beginAtZero: true,
                                title: {{
                                    display: true,
                                    text: 'Duration (ms)',
                                    font: {{ size: 10 }}
                                }},
                                ticks: {{ font: {{ size: 9 }} }}
                            }},
                            y: {{
                                stacked: true,
                                ticks: {{
                                    font: {{ size: 9 }},
                                    autoSkip: false
                                }}
                            }}
                        }}
                    }}
                }});
            }}

            // Quality Distribution Chart - Horizontal Bars with Step Labels
            if (qualityLabels.length > 0) {{
                const qualityCtx = document.getElementById('qualityChart').getContext('2d');

                new Chart(qualityCtx, {{
                    type: 'bar',
                    data: {{
                        labels: qualityLabels.map((name, i) => truncateName(name) + ' (' + qualitySteps[i] + ')'),
                        datasets: [{{
                            label: 'Quality Score',
                            data: qualityValues,
                            backgroundColor: qualityValues.map((q, i) => {{
                                if (i === 0) return 'rgba(220, 38, 38, 0.7)';
                                if (i === qualityValues.length - 1) return 'rgba(22, 163, 74, 0.7)';
                                return q >= 80 ? 'rgba(22, 163, 74, 0.5)' :
                                       q >= 60 ? 'rgba(202, 138, 4, 0.5)' :
                                       'rgba(220, 38, 38, 0.5)';
                            }}),
                            borderColor: qualityValues.map((q, i) => {{
                                if (i === 0) return 'rgba(220, 38, 38, 1)';
                                if (i === qualityValues.length - 1) return 'rgba(22, 163, 74, 1)';
                                return q >= 80 ? 'rgba(22, 163, 74, 1)' :
                                       q >= 60 ? 'rgba(202, 138, 4, 1)' :
                                       'rgba(220, 38, 38, 1)';
                            }}),
                            borderWidth: 1
                        }}]
                    }},
                    options: {{
                        indexAxis: 'y',
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {{
                            legend: {{ display: false }},
                            tooltip: {{
                                callbacks: {{
                                    title: function(context) {{
                                        return qualityLabels[context[0].dataIndex];
                                    }},
                                    label: function(context) {{
                                        const step = qualitySteps[context.dataIndex];
                                        const stepLabel = step === 'composite' ? 'Parse×0.5 + Enrich×0.25 + Rel×0.25' : step;
                                        return 'Quality: ' + context.raw.toFixed(1) + ' (' + stepLabel + ')';
                                    }}
                                }}
                            }}
                        }},
                        scales: {{
                            x: {{
                                beginAtZero: true,
                                max: 100,
                                title: {{
                                    display: true,
                                    text: 'Quality Score',
                                    font: {{ size: 10 }}
                                }},
                                ticks: {{ font: {{ size: 9 }} }}
                            }},
                            y: {{
                                ticks: {{
                                    font: {{ size: 9 }},
                                    autoSkip: false
                                }}
                            }}
                        }}
                    }}
                }});
            }}

            // Entity Type Distribution Chart (Doughnut)
            const entityTypeLabels = {entity_type_labels_data};
            const entityTypeCounts = {entity_type_counts_data};
            const entityTypeColors = {entity_type_colors_data};

            if (entityTypeLabels.length > 0) {{
                const entityTypeCtx = document.getElementById('entityTypeChart');
                if (entityTypeCtx) {{
                    new Chart(entityTypeCtx, {{
                        type: 'doughnut',
                        data: {{
                            labels: entityTypeLabels.map(l => l.charAt(0).toUpperCase() + l.slice(1)),
                            datasets: [{{
                                data: entityTypeCounts,
                                backgroundColor: entityTypeColors,
                                borderWidth: 2,
                                borderColor: '#ffffff'
                            }}]
                        }},
                        options: {{
                            responsive: true,
                            maintainAspectRatio: false,
                            plugins: {{
                                legend: {{
                                    position: 'right',
                                    labels: {{
                                        boxWidth: 12,
                                        padding: 10,
                                        font: {{ size: 11 }}
                                    }}
                                }},
                                tooltip: {{
                                    callbacks: {{
                                        label: function(context) {{
                                            const total = context.dataset.data.reduce((a, b) => a + b, 0);
                                            const pct = ((context.raw / total) * 100).toFixed(1);
                                            return context.label + ': ' + context.raw + ' (' + pct + '%)';
                                        }}
                                    }}
                                }}
                            }}
                        }}
                    }});
                }}
            }}

            // Central Entities Chart (Horizontal Bar)
            const centralEntityNames = {central_entity_names_data};
            const centralEntityCounts = {central_entity_counts_data};

            if (centralEntityNames.length > 0) {{
                const centralCtx = document.getElementById('centralEntitiesChart');
                if (centralCtx) {{
                    new Chart(centralCtx, {{
                        type: 'bar',
                        data: {{
                            labels: centralEntityNames.map(n => n.length > 20 ? n.substring(0, 20) + '...' : n),
                            datasets: [{{
                                label: 'Connections',
                                data: centralEntityCounts,
                                backgroundColor: 'rgba(124, 58, 237, 0.7)',
                                borderColor: 'rgba(124, 58, 237, 1)',
                                borderWidth: 1
                            }}]
                        }},
                        options: {{
                            indexAxis: 'y',
                            responsive: true,
                            maintainAspectRatio: false,
                            plugins: {{
                                legend: {{ display: false }},
                                tooltip: {{
                                    callbacks: {{
                                        title: function(context) {{
                                            return centralEntityNames[context[0].dataIndex];
                                        }},
                                        label: function(context) {{
                                            return 'Connections: ' + context.raw;
                                        }}
                                    }}
                                }}
                            }},
                            scales: {{
                                x: {{
                                    beginAtZero: true,
                                    title: {{
                                        display: true,
                                        text: 'Number of Connections',
                                        font: {{ size: 10 }}
                                    }},
                                    ticks: {{ font: {{ size: 9 }} }}
                                }},
                                y: {{
                                    ticks: {{
                                        font: {{ size: 9 }},
                                        autoSkip: false
                                    }}
                                }}
                            }}
                        }}
                    }});
                }}
            }}

            // Initialize Entity Network (Vis.js)
            initEntityNetwork();

        }});

        // Entity Network Functions
        function initEntityNetwork() {{
            const container = document.getElementById('entity-network-container');
            if (!container || entityNodes.length === 0) return;

            allNodes = new vis.DataSet(entityNodes);
            allEdges = new vis.DataSet(entityEdges);

            const data = {{
                nodes: allNodes,
                edges: allEdges
            }};

            const options = {{
                nodes: {{
                    shape: 'dot',
                    font: {{
                        size: 11,
                        color: '#333333'
                    }},
                    borderWidth: 2,
                    shadow: true
                }},
                edges: {{
                    width: 1,
                    color: {{
                        color: '#cccccc',
                        highlight: '#7c3aed',
                        hover: '#7c3aed'
                    }},
                    smooth: {{
                        type: 'continuous'
                    }}
                }},
                physics: {{
                    enabled: true,
                    solver: 'forceAtlas2Based',
                    forceAtlas2Based: {{
                        gravitationalConstant: -50,
                        centralGravity: 0.01,
                        springLength: 100,
                        springConstant: 0.08
                    }},
                    stabilization: {{
                        iterations: 150,
                        updateInterval: 25
                    }}
                }},
                interaction: {{
                    hover: true,
                    tooltipDelay: 100,
                    zoomView: true,
                    dragView: true
                }}
            }};

            entityNetwork = new vis.Network(container, data, options);

            // Click handler for node details
            entityNetwork.on('click', function(params) {{
                if (params.nodes.length > 0) {{
                    showEntityDetails(params.nodes[0]);
                }}
            }});
        }}

        function filterEntityNetwork() {{
            if (!entityNetwork || !allNodes) return;

            const typeFilter = document.getElementById('entityTypeFilter').value;
            const minConnections = parseInt(document.getElementById('minConnections').value);
            document.getElementById('minConnectionsValue').textContent = minConnections;

            // Filter nodes
            const visibleNodeIds = new Set();
            entityNodes.forEach(node => {{
                const matchesType = typeFilter === 'all' || node.type === typeFilter;
                const connections = entityEdges.filter(e => e.from === node.id || e.to === node.id)
                    .reduce((sum, e) => sum + e.value, 0);
                const matchesConnections = connections >= minConnections;

                if (matchesType && matchesConnections) {{
                    visibleNodeIds.add(node.id);
                }}
            }});

            // Update nodes visibility
            const updatedNodes = entityNodes.map(node => ({{
                ...node,
                hidden: !visibleNodeIds.has(node.id)
            }}));
            allNodes.update(updatedNodes);

            // Update edges visibility
            const updatedEdges = entityEdges.map(edge => ({{
                ...edge,
                hidden: !visibleNodeIds.has(edge.from) || !visibleNodeIds.has(edge.to)
            }}));
            allEdges.update(updatedEdges);
        }}

        function toggleLabels() {{
            if (!entityNetwork) return;

            const showLabels = document.getElementById('showLabels').checked;
            const updatedNodes = entityNodes.map(node => ({{
                ...node,
                font: {{
                    size: showLabels ? 11 : 0
                }}
            }}));
            allNodes.update(updatedNodes);
        }}

        function showEntityDetails(nodeId) {{
            const node = entityNodes.find(n => n.id === nodeId);
            if (!node) return;

            const panel = document.getElementById('entityDetailsPanel');
            const content = document.getElementById('entityDetailsContent');

            const typeClass = 'entity-type-' + node.type;
            const docsHtml = node.documents.map(d =>
                '<span class="entity-details-doc">' + d + '</span>'
            ).join('');

            content.innerHTML = `
                <div class="entity-details-title">${{node.fullName}}</div>
                <span class="entity-details-type ${{typeClass}}">${{node.type}}</span>

                <div class="entity-details-section">
                    <div class="entity-details-section-title">Statistics</div>
                    <div class="entity-details-stat">
                        <span>Documents</span>
                        <span>${{node.docCount}}</span>
                    </div>
                    <div class="entity-details-stat">
                        <span>Connections</span>
                        <span>${{entityEdges.filter(e => e.from === nodeId || e.to === nodeId).length}}</span>
                    </div>
                </div>

                <div class="entity-details-section">
                    <div class="entity-details-section-title">Appears In</div>
                    ${{docsHtml}}
                </div>
            `;

            panel.classList.add('visible');
        }}

        function hideEntityDetails() {{
            document.getElementById('entityDetailsPanel').classList.remove('visible');
        }}
    </script>
</body>
</html>
'''

    return html
