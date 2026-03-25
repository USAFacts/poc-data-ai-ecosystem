"""Asset report API routes - detailed workflow/step information like HTML report."""

import json
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel
from sqlalchemy import select

from src.api.deps import DBSession, Storage
from src.models.domain import AgencyModel, AssetModel, WorkflowModel

router = APIRouter()

# Zone constants
LANDING_ZONE = "landing-zone"
PARSED_ZONE = "parsed-zone"
CHUNK_ZONE = "chunk-zone"
ENRICHMENT_ZONE = "enrichment-zone"


class StepReport(BaseModel):
    """Report data for a single step."""
    name: str
    type: str
    status: str  # "success", "failed", "pending", "not_run"
    run_id: str | None = None  # UTID linking all artifacts in a single run
    zone: str | None = None
    object_path: str | None = None
    object_size: int | None = None
    duration_ms: int | None = None
    # Onboarding fields
    description: str | None = None
    labels: dict[str, str] | None = None
    source_url: str | None = None
    schedule: str | None = None
    # Acquisition fields
    file_format: str | None = None
    acquisition_method: str | None = None
    # Parse fields
    parser_type: str | None = None
    page_count: int | None = None  # Pages for PDF, sheets for Excel
    quality_scores: dict[str, float] | None = None
    document_type: str | None = None
    table_count: int | None = None
    section_count: int | None = None
    token_count: int | None = None
    tables_quality: list[dict[str, Any]] | None = None  # Table quality metrics
    text_structure: dict[str, Any] | None = None  # Text structure metrics
    mime_type: str | None = None  # To distinguish PDF vs Excel
    # Chunk fields
    chunk_count: int | None = None
    document_chunks: int | None = None
    section_chunks: int | None = None
    table_chunks: int | None = None
    # Enrichment fields
    enricher_type: str | None = None
    enrichment_model: str | None = None
    entity_count: int | None = None
    topic_count: int | None = None
    enrichment_quality_score: float | None = None
    has_embedding: bool | None = None
    # Sync fields
    weaviate_synced: bool | None = None
    neo4j_synced: bool | None = None


class WorkflowReport(BaseModel):
    """Report data for a workflow."""
    name: str
    asset_name: str
    agency_name: str
    agency_full_name: str
    run_id: str | None = None  # UTID of the latest run
    steps: list[StepReport]
    overall_status: str = "not_run"
    quality_score: float | None = None
    parse_quality_score: float | None = None
    enrichment_quality_score: float | None = None
    total_duration_ms: int | None = None
    last_run: str | None = None
    file_format: str | None = None


class AssetReportResponse(BaseModel):
    """Response containing all workflow reports."""
    workflows: list[WorkflowReport]
    filters: dict[str, list[str]]
    generated_at: str


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


def _get_latest_version(storage: Storage, zone: str, agency: str, asset: str) -> tuple[str | None, datetime | None, int | None]:
    """Get the latest version for an asset in a zone."""
    try:
        prefix = f"{zone}/{agency}/{asset}/"
        objects = list(storage.client.list_objects(storage.bucket, prefix=prefix, recursive=True))

        if not objects:
            return None, None, None

        latest = max(objects, key=lambda x: x.last_modified)
        object_size = latest.size

        path_parts = latest.object_name.split("/")
        if len(path_parts) >= 5:
            datestamp = path_parts[3]
            timestamp = path_parts[4]
            version = f"{datestamp}/{timestamp}"
            return version, latest.last_modified, object_size

        return None, latest.last_modified, object_size
    except Exception:
        return None, None, None


def _get_object_metadata(storage: Storage, object_path: str) -> dict[str, str] | None:
    """Get object metadata including duration_ms."""
    try:
        stat = storage.client.stat_object(storage.bucket, object_path)
        return dict(stat.metadata) if stat.metadata else None
    except Exception:
        return None


def _calculate_enrichment_quality_score(enrichment: dict[str, Any] | None) -> float | None:
    """Calculate enrichment quality score (0-100) from enrichment data."""
    if not enrichment:
        return None

    document = enrichment.get("document", {})
    tables = enrichment.get("tables", [])
    sections = enrichment.get("sections", [])

    # Entity Coverage (0-25 points)
    entities = document.get("entities", [])
    entity_count = len(entities)
    entity_types = len(set(e.get("type", "other") for e in entities)) if entities else 0
    entity_count_score = min(entity_count / 5, 1.0) * 15
    entity_diversity_score = min(entity_types / 3, 1.0) * 10
    entity_score = entity_count_score + entity_diversity_score

    # Topic Completeness (0-25 points)
    topics = document.get("keyTopics", [])
    topic_count = len(topics)
    topic_score = min(topic_count / 5, 1.0) * 25

    # Summary Quality (0-25 points)
    summary = document.get("summary", "")
    if summary:
        length_factor = min(len(summary) / 200, 1.0)
        summary_score = 10 + (length_factor * 15)
    else:
        summary_score = 0

    # RAG Readiness (0-25 points)
    example_queries = document.get("exampleQueries", [])
    query_score = min(len(example_queries) / 3, 1.0) * 15

    table_enrichment = sum(1 for t in tables if t.get("description"))
    section_enrichment = sum(1 for s in sections if s.get("summary"))
    enrichment_items = table_enrichment + section_enrichment
    structure_score = min(enrichment_items / 3, 1.0) * 10

    rag_score = query_score + structure_score

    total_score = entity_score + topic_score + summary_score + rag_score
    return min(total_score, 100.0)


def _collect_workflow_reports(db: DBSession, storage: Storage) -> list[WorkflowReport]:
    """Collect report data for all workflows."""
    reports = []

    # Get all workflows with their assets and agencies
    stmt = (
        select(WorkflowModel, AssetModel, AgencyModel)
        .join(AssetModel, WorkflowModel.asset_id == AssetModel.id)
        .join(AgencyModel, AssetModel.agency_id == AgencyModel.id)
        .order_by(AgencyModel.name, AssetModel.name)
    )
    results = db.execute(stmt).all()

    for workflow, asset, agency in results:
        report = WorkflowReport(
            name=workflow.name,
            asset_name=asset.name,
            agency_name=agency.name,
            agency_full_name=agency.full_name,
            steps=[],
            overall_status="not_run",
        )

        # Add implicit onboarding step
        labels_dict = None
        if asset.labels:
            labels_dict = {str(k): str(v) for k, v in asset.labels.items()}

        source_url = None
        schedule = None
        acq_config = asset.acquisition_config or {}
        if isinstance(acq_config, dict):
            source = acq_config.get("source", {})
            if isinstance(source, dict):
                source_url = source.get("url")
            schedule = acq_config.get("schedule")

        onboarding_step = StepReport(
            name="Onboarding",
            type="onboarding",
            status="success",
            description=asset.description,
            labels=labels_dict,
            source_url=source_url,
            schedule=schedule,
        )
        report.steps.append(onboarding_step)

        has_success = True
        has_failure = False
        total_duration_ms = 0
        has_duration = False
        file_format = None

        # Process each step from workflow definition
        for step_def in workflow.steps:
            step_name = step_def.get("name", "unknown")
            step_type = step_def.get("type", "unknown")

            step_report = StepReport(
                name=step_name,
                type=step_type,
                status="not_run",
            )

            if step_type == "acquisition":
                # Get acquisition config
                file_format = acq_config.get("format", "").upper()
                acq_type = acq_config.get("type", "http")
                step_report.file_format = file_format
                step_report.acquisition_method = acq_type
                report.file_format = file_format

                version, last_modified, object_size = _get_latest_version(
                    storage, LANDING_ZONE, agency.name, asset.name
                )

                if version:
                    step_report.status = "success"
                    step_report.zone = LANDING_ZONE
                    step_report.object_path = f"{LANDING_ZONE}/{agency.name}/{asset.name}/{version}"
                    step_report.object_size = object_size
                    has_success = True

                    # Get duration and run_id from metadata
                    files = list(storage.client.list_objects(
                        storage.bucket,
                        prefix=f"{LANDING_ZONE}/{agency.name}/{asset.name}/{version}/",
                        recursive=True
                    ))
                    if files:
                        metadata = _get_object_metadata(storage, files[0].object_name)
                        if metadata:
                            duration_str = metadata.get("x-amz-meta-duration_ms") or metadata.get("duration_ms")
                            if duration_str:
                                try:
                                    step_report.duration_ms = int(duration_str)
                                    total_duration_ms += step_report.duration_ms
                                    has_duration = True
                                except (ValueError, TypeError):
                                    pass
                            # Extract run_id from object metadata
                            rid = metadata.get("x-amz-meta-run_id") or metadata.get("run_id")
                            if rid:
                                step_report.run_id = rid
                                report.run_id = rid

                    if last_modified:
                        report.last_run = last_modified.isoformat()

            elif step_type == "parse":
                version, last_modified, object_size = _get_latest_version(
                    storage, PARSED_ZONE, agency.name, asset.name
                )

                if version:
                    step_report.status = "success"
                    step_report.zone = PARSED_ZONE
                    step_report.object_path = f"{PARSED_ZONE}/{agency.name}/{asset.name}/{version}"
                    step_report.object_size = object_size
                    has_success = True

                    # Get parsed document data
                    object_path = f"{PARSED_ZONE}/{agency.name}/{asset.name}/{version}/{asset.name}.json"
                    doc = storage.get_json_object(object_path)
                    if doc:
                        quality = doc.get("quality", {})
                        scores = quality.get("scores", {})
                        step_report.quality_scores = scores
                        report.parse_quality_score = scores.get("overall")

                        content = quality.get("content", {})
                        step_report.document_type = content.get("documentType")
                        step_report.table_count = content.get("tableCount")
                        step_report.section_count = content.get("sectionCount")
                        step_report.token_count = content.get("estimatedTokens")

                        # Table quality metrics
                        tables_quality = quality.get("tables", [])
                        if tables_quality:
                            step_report.tables_quality = tables_quality

                        # Text structure metrics
                        text_metrics = quality.get("text", {})
                        if text_metrics:
                            step_report.text_structure = text_metrics

                        extraction = doc.get("extraction", {})
                        step_report.parser_type = extraction.get("parser")

                        # Page count comes from source (pages for PDF, sheets for Excel)
                        source = doc.get("source", {})
                        step_report.page_count = source.get("pageCount")
                        step_report.mime_type = source.get("mimeType")

                        # Extract run_id from document body
                        rid = source.get("run_id")
                        if rid:
                            step_report.run_id = rid
                            report.run_id = rid

                        processing_time_ms = extraction.get("processingTimeMs")
                        if processing_time_ms:
                            step_report.duration_ms = processing_time_ms
                            total_duration_ms += processing_time_ms
                            has_duration = True

                    if last_modified:
                        report.last_run = last_modified.isoformat()

            elif step_type == "enrichment":
                version, last_modified, object_size = _get_latest_version(
                    storage, ENRICHMENT_ZONE, agency.name, asset.name
                )

                if version:
                    step_report.status = "success"
                    step_report.zone = ENRICHMENT_ZONE
                    step_report.object_path = f"{ENRICHMENT_ZONE}/{agency.name}/{asset.name}/{version}"
                    step_report.object_size = object_size
                    has_success = True

                    # Get enriched document data
                    object_path = f"{ENRICHMENT_ZONE}/{agency.name}/{asset.name}/{version}/{asset.name}.json"
                    doc = storage.get_json_object(object_path)
                    if doc:
                        enrichment = doc.get("enrichment", {})
                        enrichment_info = enrichment.get("enrichmentInfo", {})

                        step_report.enricher_type = enrichment_info.get("enricher")
                        step_report.enrichment_model = enrichment_info.get("model")

                        document = enrichment.get("document", {})
                        step_report.entity_count = len(document.get("entities", []))
                        step_report.topic_count = len(document.get("keyTopics", []))

                        processing_time_ms = enrichment_info.get("processingTimeMs")
                        if processing_time_ms:
                            step_report.duration_ms = processing_time_ms
                            total_duration_ms += processing_time_ms
                            has_duration = True

                        # Calculate enrichment quality score
                        enrichment_quality = _calculate_enrichment_quality_score(enrichment)
                        if enrichment_quality:
                            step_report.enrichment_quality_score = enrichment_quality
                            report.enrichment_quality_score = enrichment_quality

                    # Extract run_id from enrichment object metadata
                    enrich_metadata = _get_object_metadata(storage, object_path)
                    if enrich_metadata:
                        rid = enrich_metadata.get("x-amz-meta-run_id") or enrich_metadata.get("run_id")
                        if rid:
                            step_report.run_id = rid
                            report.run_id = rid

                    if last_modified:
                        report.last_run = last_modified.isoformat()

            elif step_type == "chunk":
                version, last_modified, object_size = _get_latest_version(
                    storage, CHUNK_ZONE, agency.name, asset.name
                )

                if version:
                    step_report.status = "success"
                    step_report.zone = CHUNK_ZONE
                    step_report.object_path = f"{CHUNK_ZONE}/{agency.name}/{asset.name}/{version}"
                    step_report.object_size = object_size
                    has_success = True

                    # Load chunk document to get counts
                    chunk_path = f"{CHUNK_ZONE}/{agency.name}/{asset.name}/{version}/{asset.name}_chunks.json"
                    chunk_doc = storage.get_json_object(chunk_path)
                    if chunk_doc:
                        chunks = chunk_doc.get("chunks", [])
                        step_report.chunk_count = len(chunks)
                        step_report.document_chunks = sum(1 for c in chunks if c.get("level") == "document")
                        step_report.section_chunks = sum(1 for c in chunks if c.get("level") == "section")
                        step_report.table_chunks = sum(1 for c in chunks if c.get("level") == "table")

                    # Extract run_id from metadata
                    chunk_files = list(storage.client.list_objects(
                        storage.bucket,
                        prefix=f"{CHUNK_ZONE}/{agency.name}/{asset.name}/{version}/",
                        recursive=True
                    ))
                    if chunk_files:
                        chunk_metadata = _get_object_metadata(storage, chunk_files[0].object_name)
                        if chunk_metadata:
                            rid = chunk_metadata.get("x-amz-meta-run_id") or chunk_metadata.get("run_id")
                            if rid:
                                step_report.run_id = rid

                    if last_modified:
                        report.last_run = last_modified.isoformat()

            elif step_type == "sync":
                # Sync status is determined by checking if the document exists
                # in both Weaviate and Neo4j
                enrichment_version, _, _ = _get_latest_version(
                    storage, ENRICHMENT_ZONE, agency.name, asset.name
                )
                chunk_version, _, _ = _get_latest_version(
                    storage, CHUNK_ZONE, agency.name, asset.name
                )

                # If enrichment exists, sync should have run
                if enrichment_version:
                    step_report.status = "success"
                    has_success = True

                    # Check Weaviate
                    try:
                        from src.services import weaviate_client
                        client = weaviate_client.get_client()
                        doc_collection = client.collections.get(weaviate_client.GOV_DOCUMENT_COLLECTION)
                        doc_id = f"{agency.name}/{asset.name}/{enrichment_version}"
                        import uuid as _uuid
                        doc_uuid = _uuid.uuid5(_uuid.NAMESPACE_URL, doc_id)
                        obj = doc_collection.query.fetch_object_by_id(doc_uuid)
                        step_report.weaviate_synced = obj is not None
                    except Exception:
                        step_report.weaviate_synced = False

                    # Check Neo4j
                    try:
                        from src.services import neo4j_client
                        driver = neo4j_client.get_driver()
                        with driver.session() as session:
                            result = session.run(
                                "MATCH (d:Document {asset: $asset, agency: $agency}) RETURN count(d) AS c",
                                {"asset": asset.name, "agency": agency.name},
                            )
                            count = result.single()["c"]
                            step_report.neo4j_synced = count > 0
                    except Exception:
                        step_report.neo4j_synced = False

                    # Check for embedding in enriched doc
                    if enrichment_version:
                        enrich_path = f"{ENRICHMENT_ZONE}/{agency.name}/{asset.name}/{enrichment_version}/{asset.name}.json"
                        enrich_doc = storage.get_json_object(enrich_path)
                        if enrich_doc:
                            emb = enrich_doc.get("enrichment", {}).get("embedding", {})
                            step_report.has_embedding = bool(emb.get("vector"))

            report.steps.append(step_report)

        # Set total duration
        if has_duration:
            report.total_duration_ms = total_duration_ms

        # Determine overall status
        if has_success and not has_failure:
            report.overall_status = "success"
        elif has_success and has_failure:
            report.overall_status = "partial"
        elif has_failure:
            report.overall_status = "failed"
        else:
            report.overall_status = "not_run"

        # Calculate composite quality score
        scores = []
        weights = []

        if report.parse_quality_score is not None:
            scores.append(report.parse_quality_score)
            weights.append(0.6)
        if report.enrichment_quality_score is not None:
            scores.append(report.enrichment_quality_score)
            weights.append(0.4)

        if scores:
            total_weight = sum(weights)
            normalized_weights = [w / total_weight for w in weights]
            report.quality_score = sum(s * w for s, w in zip(scores, normalized_weights))

        reports.append(report)

    return reports


@router.get("/report", response_model=AssetReportResponse)
def get_asset_reports(
    db: DBSession,
    storage: Storage,
    agency: str | None = Query(None, description="Filter by agency name"),
    format: str | None = Query(None, description="Filter by file format"),
    step: str | None = Query(None, description="Filter by latest step"),
) -> AssetReportResponse:
    """Get detailed workflow reports for all assets, similar to HTML report."""
    workflows = _collect_workflow_reports(db, storage)

    # Apply filters
    if agency:
        workflows = [w for w in workflows if w.agency_name == agency]

    if format:
        workflows = [w for w in workflows if w.file_format and w.file_format.lower() == format.lower()]

    if step:
        # Filter by latest completed step
        def get_latest_step(w: WorkflowReport) -> str:
            for s in reversed(w.steps):
                if s.status == "success" and s.type != "onboarding":
                    return s.type
            return "onboarding"

        workflows = [w for w in workflows if get_latest_step(w) == step]

    # Collect filter options
    all_workflows = _collect_workflow_reports(db, storage)
    agencies = sorted(set(w.agency_name for w in all_workflows))
    formats = sorted(set(w.file_format for w in all_workflows if w.file_format))
    steps = ["onboarding", "acquisition", "parse", "chunk", "enrichment", "sync"]

    return AssetReportResponse(
        workflows=workflows,
        filters={
            "agencies": agencies,
            "formats": formats,
            "steps": steps,
        },
        generated_at=datetime.now(timezone.utc).isoformat(),
    )
