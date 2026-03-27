"""FastAPI application entry point."""

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables BEFORE importing modules that need them
# Explicitly find .env file relative to this file's location
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# Debug: print whether API key was loaded
api_key = os.getenv("ANTHROPIC_API_KEY")
print(f"[DEBUG] .env path: {env_path}")
print(f"[DEBUG] .env exists: {env_path.exists()}")
print(f"[DEBUG] ANTHROPIC_API_KEY loaded: {bool(api_key)}")

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.routes import agencies, asset_reports, assets, data, experiments, graph, metrics, neo4j_status, search, stats, weaviate_status, workflows
from src.models.domain import Base
from src.services.database import engine
from src.services.storage import get_storage_service

GRAPH_ENABLED = os.getenv("GRAPH_ENABLED", "false").lower() in ("true", "1", "yes")

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler for startup/shutdown events."""
    # Startup: Ensure database tables exist
    logger.info("[startup] Ensuring database tables exist...")
    Base.metadata.create_all(engine)

    storage = get_storage_service()

    # Weaviate search backend initialization
    logger.info("[startup] Initializing Weaviate search backend...")
    try:
        from src.services import weaviate_client

        weaviate_client.ensure_schema()
        result = weaviate_client.sync_from_storage(storage)
        logger.info(f"[startup] Weaviate ready: {result['documents']} docs, {result['chunks']} chunks")
    except Exception as e:
        logger.warning(f"[startup] Weaviate init failed: {e}")

    # Neo4j graph initialization
    if GRAPH_ENABLED:
        logger.info("[startup] Initializing Neo4j knowledge graph...")
        try:
            from src.services import neo4j_client
            from src.services import graph_builder

            neo4j_client.ensure_constraints()
            result = graph_builder.sync_from_storage(storage)
            logger.info(
                f"[startup] Neo4j ready: {result['documents']} docs, "
                f"{result['nodes']} nodes, {result['relationships']} rels"
            )
        except Exception as e:
            logger.warning(f"[startup] Neo4j init failed (graph features disabled): {e}")

    yield

    # Shutdown: cleanup
    logger.info("[shutdown] Application shutting down")
    try:
        from src.services import weaviate_client
        weaviate_client.close()
    except Exception:
        pass
    if GRAPH_ENABLED:
        try:
            from src.services import neo4j_client
            neo4j_client.close()
        except Exception:
            pass

# OpenAPI tags metadata for better documentation
tags_metadata = [
    {
        "name": "agencies",
        "description": "Operations with government agencies. Agencies are the source organizations for data assets.",
    },
    {
        "name": "assets",
        "description": "Operations with data assets. Assets represent individual data sources (documents, spreadsheets, APIs) from agencies.",
    },
    {
        "name": "asset-reports",
        "description": "Detailed workflow execution reports for assets, including step-by-step status and quality metrics.",
    },
    {
        "name": "workflows",
        "description": "Operations with data ingestion workflows. Workflows define the processing pipeline for each asset.",
    },
    {
        "name": "metrics",
        "description": "DIS (Data Ingestion Score) metrics and history. Tracks quality, efficiency, and execution success.",
    },
    {
        "name": "stats",
        "description": "Dashboard statistics and aggregated metrics from the pipeline.",
    },
    {
        "name": "search",
        "description": "Semantic search and Data Assistant powered by Claude AI. Query documents using natural language.",
    },
    {
        "name": "graph",
        "description": "Knowledge graph queries via Neo4j. Explore entity relationships, document connections, and temporal coverage.",
    },
    {
        "name": "weaviate",
        "description": "Weaviate vector database status. Collection schemas, object counts, and connection info.",
    },
    {
        "name": "neo4j",
        "description": "Neo4j knowledge graph status. Node/relationship counts, indexes, constraints, and graph schema visualization.",
    },
    {
        "name": "data",
        "description": "Data layer API. Browse documents, entities, agencies, time periods, and raw files. Structured access to all ingested and enriched data.",
    },
    {
        "name": "experiments",
        "description": "RAG evaluation experiment tracker. Run ablation tests comparing retrieval modes (V, VG, VW, VGW) across 350 stratified questions with quality metrics (STS, NVS, HDS, CSCS).",
    },
]

# Create FastAPI application
app = FastAPI(
    title="Gov Data Pipeline API",
    description="""
## Government Data AI Ecosystem — REST API

Full API for the government data ingestion, retrieval, and Q&A pipeline.

### Intelligence Platform
- **Agencies & Assets**: Browse government data sources and their pipeline configurations
- **Workflows**: View and trigger data processing pipelines
- **Quality Metrics**: DIS (Data Ingestion Score) for quality monitoring
- **Weaviate Status**: Vector database schema, collection counts, and connection health
- **Neo4j Status**: Knowledge graph schema, node/relationship counts, indexes

### Data Layer API (`/api/data/`)
- **Documents**: List, search, and access full document content with sections, tables, and chunks
- **Entities**: Browse extracted entities with mention counts and cross-document relationships
- **Time Periods**: Temporal coverage of the document collection
- **Graph Paths**: Shortest-path queries between entities in the knowledge graph
- **Raw Objects**: Download original files (PDF, XLSX, CSV) from MinIO storage

### Q&A (RAG Pipeline)
- **4 Retrieval Modes**: V (Weaviate), VG (+Graph), VW (+Web), VGW (all sources)
- **Hybrid Search**: BM25 + vector with cross-encoder reranking and trust-weighted scoring
- **Web Search**: Live Firecrawl queries against .gov and usafacts.org
- **Answer Quality**: STS, NVS, HDS, CSCS metrics per answer
- **Charts**: Auto-generated visualizations when data supports them

### Experiment Tracker
- **Ablation Testing**: Compare retrieval modes across 350 stratified questions
- **Metrics**: Confidence, relevance, entity coverage, STS, NVS, HDS, CSCS per mode
- **Stratified Sampling**: Configurable sample size (10-100%) preserving category distribution

### Storage
- MinIO object storage: `landing-zone`, `parsed-zone`, `enrichment-zone`
- PostgreSQL: pipeline metadata, workflow state, experiment results
- Weaviate: vector embeddings and hybrid search indexes
- Neo4j: knowledge graph with entities, agencies, time periods

### Authentication
Currently no authentication required (development mode).
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
    openapi_tags=tags_metadata,
    contact={
        "name": "USAFacts Data Engineering",
        "url": "https://usafacts.org",
    },
    license_info={
        "name": "MIT",
    },
)

# Configure CORS
cors_origins = os.getenv("CORS_ORIGINS", "http://localhost:5173,http://localhost:3000")
origins = [origin.strip() for origin in cors_origins.split(",")]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(agencies.router, prefix="/api/agencies", tags=["agencies"])
app.include_router(assets.router, prefix="/api/assets", tags=["assets"])
app.include_router(asset_reports.router, prefix="/api/asset-reports", tags=["asset-reports"])
app.include_router(workflows.router, prefix="/api/workflows", tags=["workflows"])
app.include_router(metrics.router, prefix="/api/metrics", tags=["metrics"])
app.include_router(stats.router, prefix="/api/stats", tags=["stats"])
app.include_router(search.router, prefix="/api/search", tags=["search"])
app.include_router(graph.router, prefix="/api/graph", tags=["graph"])
app.include_router(weaviate_status.router, prefix="/api/weaviate", tags=["weaviate"])
app.include_router(neo4j_status.router, prefix="/api/neo4j", tags=["neo4j"])
app.include_router(data.router, prefix="/api/data", tags=["data"])
app.include_router(experiments.router, prefix="/api/experiments", tags=["experiments"])


@app.get("/", tags=["root"], summary="API Information")
def root() -> dict[str, str]:
    """
    Get basic API information and documentation links.

    Returns the API name, version, and link to interactive documentation.
    """
    return {
        "name": "Gov Data Pipeline API",
        "version": "1.0.0",
        "docs": "/docs",
        "redoc": "/redoc",
        "openapi": "/openapi.json",
    }


@app.get("/health", tags=["root"], summary="Health Check")
def health_check() -> dict[str, str]:
    """
    Check if the API is healthy and responding.

    Used by load balancers and container orchestration for health monitoring.
    Returns `{"status": "healthy"}` when the service is operational.
    """
    return {"status": "healthy"}
