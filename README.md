# Government Data AI Ecosystem

A manifest-driven architecture (MDA) for government data ingestion, processing, and visualization. Built on Model_D patterns: URN-based addressing, pluggable providers, and MCP integration.

## Project Structure

```
poc-data-ai-ecosystem/
├── ingestion-processing/          # Data ingestion pipeline
│   ├── src/
│   │   ├── cli.py                 # Typer CLI entrypoint
│   │   ├── logging_manager.py     # Centralized logging (console + file + DB)
│   │   ├── control/               # Manifest compilation (Registry, Compiler, Validator)
│   │   ├── runtime/               # Execution engine (SequentialExecutor)
│   │   ├── steps/                 # Step implementations (acquisition, parse, enrichment)
│   │   ├── storage/               # MinIO client and zone naming
│   │   ├── db/                    # Database models, sync, and repositories (PostgreSQL)
│   │   ├── mda/                   # MDA framework (Model_D patterns)
│   │   │   ├── capability/        # CapabilityInterface ABC + adapter
│   │   │   ├── interpreter/       # StandardInterpreter (universal execution loop)
│   │   │   ├── orchestrator/      # WorkflowExecutor + ManifestExecutor
│   │   │   ├── resolver/          # DefaultResolver (URN -> capability class)
│   │   │   ├── manifest/          # ParserInterface, URN utils, mapper
│   │   │   ├── migration/         # pipeline/v1 -> standard/1.0 converter
│   │   │   └── traceability/      # UTID minting
│   │   ├── providers/             # MDA provider plugins
│   │   │   ├── mda_ingestion_provider/   # Wraps existing steps as capabilities
│   │   │   └── mda_semantics_provider/   # MCP-based embeddings
│   │   ├── mcp_servers/           # MCP server implementations
│   │   │   └── embedding_server/  # FastMCP embedding server
│   │   ├── services/              # Shared services (embeddings)
│   │   └── reports/               # HTML report generation
│   ├── manifests/                 # YAML configurations (agencies, assets, workflows)
│   └── tests/                     # Test suite
│
├── backend/                       # FastAPI REST API
│   └── src/
│       ├── main.py                # FastAPI app with lifespan handlers
│       ├── api/routes/            # API endpoints (agencies, assets, workflows, search)
│       ├── models/                # SQLAlchemy ORM + Pydantic schemas
│       └── services/              # Database, storage, embeddings, reranker
│
├── frontend/                      # React + TypeScript UI
│   └── src/
│       ├── App.tsx                # Router setup
│       ├── pages/                 # Dashboard, Agencies, Assets, Chat
│       ├── components/            # Layout, sidebar, metrics cards
│       ├── api/                   # Typed Axios API client
│       └── types/                 # TypeScript interfaces
│
├── analysis/                      # Jupyter analysis layer
│   ├── src/
│   │   ├── analysis_helpers/      # Data explorer, loaders, SQL utilities
│   │   └── metadata_assistant/    # LLM-powered metadata curation
│   └── notebooks/                 # Example notebooks
│
├── scripts/                       # Utility scripts
│   └── init-db.sql                # PostgreSQL bootstrap (pipeline_logs table)
│
└── docker-compose.yml             # Full stack orchestration
```

## Quick Start

### Option 1: Full Stack with Docker

Run the entire application stack:

```bash
# Start all services
docker compose up -d

# Access points:
#   Frontend UI        → http://localhost:3000
#   Backend API docs   → http://localhost:8000/docs
#   MinIO console      → http://localhost:9001
#   JupyterLab         → http://localhost:8888
```

### Option 2: Development Setup

#### 1. Ingestion Processing

```bash
cd ingestion-processing

# Install dependencies
uv sync

# Copy environment template and configure
cp .env.example .env
# Edit .env — set DATABASE_URL, API keys, etc.

# Start PostgreSQL and MinIO (from project root)
cd .. && docker compose up postgres minio minio-init -d && cd ingestion-processing

# Validate manifests
uv run pipeline validate

# Sync manifests to database (populates the API/frontend)
uv run pipeline db sync

# Run a workflow
uv run pipeline run uscis-forms-pipeline
```

#### 2. Backend API

```bash
cd backend

# Install dependencies
uv sync

# Copy environment template
cp .env.example .env

# Start the API server
uv run uvicorn src.main:app --reload

# API available at http://localhost:8000
# Swagger docs at http://localhost:8000/docs
```

#### 3. Frontend UI

```bash
cd frontend

# Install dependencies
npm install

# Start development server
npm run dev

# UI available at http://localhost:5173
```

## Architecture

### How the Commands Relate

```
                 ┌── db sync ──→ PostgreSQL ──→ Backend API / Frontend
manifests/*.yaml │
                 └── run ──────→ (auto-compile) → execute → MinIO + metrics
```

- **`db sync`** loads manifest metadata into PostgreSQL so the API and UI can display agencies, assets, and workflows.
- **`run`** compiles and executes a workflow — compilation happens automatically.
- **`compile`** is optional, for inspecting execution plans without running them.

You do **not** need to compile before syncing. They are independent paths.

### MDA Execution Flow

```
YAML Manifests → Registry → Compiler → ExecutionPlan
                                              │
                                      PlanBasedParser
                                              │
                                    StandardInterpreter
                                              │
                                      DefaultResolver
                                              │
                             CapabilityAdapter → Step.execute()
```

All execution flows through the MDA interpreter chain. The `DefaultResolver` resolves
capability URNs (e.g., `cap://mda_ingestion_provider:python_v0:acquisition/v1/default_acquisition`)
to Python classes via convention-based import paths under `providers/`.

### Data Flow

```
External Sources → Acquisition Step → MinIO landing-zone/ (raw files)
                                              │
                                        Parse Step → MinIO parsed-zone/ (structured JSON)
                                              │
                                      Enrichment Step → MinIO enrichment-zone/ (embeddings + metadata)
                                              │
                              ┌────────────────┴────────────────┐
                              │                                 │
                     PostgreSQL (metadata)              Weaviate (vectors + BM25)
                              │                                 │
                         Neo4j (graph)                          │
                              │                                 │
                        Backend API ◄───────────────────────────┘
                              │
                        Frontend UI
```

### Ingestion Processing Layer

Manifest-driven pipeline for ingesting government data:

- **Control Plane**: Compiles YAML manifests into execution plans (Registry, Compiler, Validator)
- **MDA Framework**: Universal interpreter loop with URN-based capability resolution
- **Providers**: Pluggable capability providers (ingestion, semantics via MCP)
- **Steps**: Acquisition, Parse, Enrichment (wrapped as MDA capabilities)
- **Storage**: MinIO object storage with zone-based organization
- **Logging**: Centralized logging manager with console, file, and database backends
- **Traceability**: UTID (Universal Trace ID) linking all artifacts per execution run

### Backend API Layer

FastAPI REST API exposing pipeline data:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/agencies` | GET | List all agencies |
| `/api/agencies/{name}` | GET | Get agency details |
| `/api/assets` | GET | List all assets |
| `/api/assets/{name}` | GET | Get asset details |
| `/api/workflows` | GET | List all workflows |
| `/api/workflows/{name}` | GET | Get workflow details |
| `/api/workflows/{name}/run` | POST | Trigger workflow |
| `/api/metrics` | GET | Get DIS metrics |
| `/api/metrics/history` | GET | Get metrics history |
| `/api/stats/dashboard` | GET | Dashboard statistics |
| `/api/search/query` | POST | Natural language search (Data Assistant) |
| `/api/search/index-status` | GET | Weaviate index status |
| `/api/search/refresh-index` | POST | Re-sync Weaviate index |
| `/api/graph/entity/{name}/related` | GET | Related entities via graph |
| `/api/graph/entity/{name}/documents` | GET | Documents mentioning entity |
| `/api/graph/document/{id}/context` | GET | Document graph neighborhood |
| `/api/graph/stats` | GET | Knowledge graph statistics |

### API Documentation

| Format | URL | Description |
|--------|-----|-------------|
| **Swagger UI** | http://localhost:8000/docs | Interactive API explorer with "Try it out" |
| **ReDoc** | http://localhost:8000/redoc | Clean, readable API reference |
| **OpenAPI JSON** | http://localhost:8000/openapi.json | Raw OpenAPI 3.0 spec |

### Frontend UI Layer

React + TypeScript single-page application:

- **Dashboard**: Pipeline health overview and DIS metrics
- **Assets Report**: Detailed workflow execution status
- **Agencies**: Agency and asset management
- **Data Assistant**: Natural language Q&A with source attribution

### Data Assistant (RAG Search)

The Data Assistant provides natural language search over ingested documents using a RAG (Retrieval-Augmented Generation) approach:

**Features:**
- Hybrid search via Weaviate (BM25 + vector with 384-dim embeddings)
- Knowledge graph expansion via Neo4j (related entities, temporal context)
- AI-generated answers powered by Claude (Anthropic)
- Full source attribution with document citations
- Query decomposition showing extracted entities and intent

**Example queries:**
- "What is the H-1B visa program?"
- "How many I-130 forms were processed in FY2024?"
- "Compare DACA and TPS programs"

**Index Management:**
After running the pipeline, refresh the search index to include new documents:
```bash
# Via API
curl -X POST http://localhost:8000/api/search/refresh-index
```

### Analysis Layer

JupyterLab environment for interactive data exploration:

- **Data Explorer**: Browse and load documents from MinIO storage
- **Metadata Assistant**: LLM-powered metadata curation with suggestions, validation, and relationship inference
- **Example Notebooks**: Getting started, table extraction, metadata curation

## CLI Commands

All commands run from the `ingestion-processing/` directory using `uv run pipeline`.

### Validate

Validate manifest files and workflow configurations.

```bash
uv run pipeline validate              # Validate all workflows
uv run pipeline validate <workflow>   # Validate a specific workflow
```

### Run

Execute one or more workflow pipelines.

```bash
uv run pipeline run <workflow>             # Run a single workflow
uv run pipeline run --all                  # Run all workflows
uv run pipeline run --all --parallel 3     # Run all workflows with 3 workers
uv run pipeline run <workflow> --dry-run   # Validate without executing
uv run pipeline run <workflow> --json      # Output result as JSON
```

### Run Step

Run a specific step type (acquire or parse) across workflows.

```bash
uv run pipeline run-step acquire                        # Acquire all workflows
uv run pipeline run-step parse                          # Parse all workflows
uv run pipeline run-step parse --workflow <workflow>    # Parse a specific workflow
uv run pipeline run-step all                            # Run all steps (same as run --all)
uv run pipeline run-step acquire --dry-run              # Dry run
```

### List

List registered resources from manifest files.

```bash
uv run pipeline list              # List all agencies, assets, and workflows
uv run pipeline list agencies     # List agencies only
uv run pipeline list assets       # List assets only
uv run pipeline list workflows    # List workflows only
```

### Compile

Compile a workflow into an execution plan (shows steps, dependencies, and order).

```bash
uv run pipeline compile <workflow>          # Display execution plan
uv run pipeline compile <workflow> --json   # Output plan as JSON
```

### Storage

Interact with MinIO storage.

```bash
uv run pipeline storage list                                  # List all stored assets
uv run pipeline storage list --agency <agency>                # Filter by agency
uv run pipeline storage versions --agency <agency> --asset <asset>   # List asset versions
```

### Report

Generate an HTML status report with quality metrics.

```bash
uv run pipeline report                              # Generate report
uv run pipeline report --output ./my-report.html    # Custom output path
uv run pipeline report --open                       # Open in browser after generation
```

### Migrate

Convert pipeline/v1 manifests to standard/1.0 (MDA) format.

```bash
uv run pipeline migrate <workflow>         # Migrate a single workflow
uv run pipeline migrate --all              # Migrate all workflows
uv run pipeline migrate --all --output ./mda-manifests  # Custom output dir
```

Migrated manifests are written to `manifests/mda/` by default. Both formats are supported
indefinitely by the DB sync and execution engine.

### Database Commands (`db`)

Manage the PostgreSQL database used by the backend API.

```bash
# Sync manifests to database (supports both pipeline/v1 and standard/1.0)
uv run pipeline db sync

# List entities from database
uv run pipeline db list              # List all
uv run pipeline db list agencies     # List agencies only
uv run pipeline db list workflows    # List workflows only

# Generate execution plan from database
uv run pipeline db plan <workflow>          # Single workflow
uv run pipeline db plan --all              # All workflows
uv run pipeline db plan <workflow> --json  # Output as JSON

# Show sync history
uv run pipeline db status              # Last 20 entries
uv run pipeline db status --last 50    # Last 50 entries
```

### Log Commands (`logs`)

Query pipeline logs stored in the database. Requires `PIPELINE_LOG_DB=true` in your `.env`.

```bash
# 1. Discover what steps have been logged
uv run pipeline logs list-steps

# Example output:
#   ┌──────────────┬───────────┬─────────────────────┐
#   │ Step         │ Log Count │ Last Seen           │
#   ├──────────────┼───────────┼─────────────────────┤
#   │ db-sync      │       126 │ 2026-03-17 17:29:03 │
#   │ acquisition  │        42 │ 2026-03-17 17:28:00 │
#   │ parse        │        38 │ 2026-03-17 17:28:00 │
#   │ enrichment   │        12 │ 2026-03-17 17:25:00 │
#   └──────────────┴───────────┴─────────────────────┘

# 2. Show recent logs — combine filters as needed
uv run pipeline logs show                                          # All recent logs
uv run pipeline logs show --level ERROR                            # Errors only
uv run pipeline logs show --step db-sync                           # Filter by step
uv run pipeline logs show --step acquisition --level ERROR         # Step + level
uv run pipeline logs show --workflow uscis-forms-pipeline          # Filter by workflow
uv run pipeline logs show --workflow uscis-forms-pipeline --step parse  # Workflow + step
uv run pipeline logs show --search "timeout" --last 20             # Search message text

# 3. Live tail (polls the database)
uv run pipeline logs tail
uv run pipeline logs tail --level ERROR --interval 1
uv run pipeline logs tail --workflow uscis-forms-pipeline

# 4. Clear old log entries
uv run pipeline logs clear --before-days 30
uv run pipeline logs clear --yes              # Skip confirmation
```

**Step values** are recorded automatically by each pipeline operation:

| Step | Logged by |
|------|-----------|
| `db-sync` | `pipeline db sync` — manifest sync to PostgreSQL |
| `acquisition` | Workflow acquire step — fetching data from sources |
| `parse` | Workflow parse step — converting raw files to structured JSON |
| `enrichment` | Workflow enrichment step — LLM-powered semantic enrichment |

Use `logs list-steps` to see which values are currently in your database, then filter with `--step`.

## Logging

The pipeline uses a centralized logging manager (`logging_manager.py`) that provides consistent, structured logging across all components.

### Log Levels

| Level | Color | Use |
|-------|-------|-----|
| INFO | Cyan | Normal operations (workflow started, step completed) |
| WARNING | Yellow | Non-fatal issues (slow response, missing optional config) |
| ERROR | Red | Failures (step failed, connection error) |

### Log Outputs

| Output | Enabled by | Description |
|--------|------------|-------------|
| **Console** | Always | Color-coded output to stderr |
| **File** | `PIPELINE_LOG_DIR` | Daily log files (`pipeline_YYYYMMDD.log`) |
| **Database** | `PIPELINE_LOG_DB=true` | PostgreSQL `pipeline_logs` table with structured fields |

### Usage in Code

```python
from logging_manager import get_logger

logger = get_logger(__name__)
logger.info("Processing started", extra={"workflow": "uscis-forms"})
logger.warning("Slow response", extra={"step": "acquire", "duration_s": 12.5})
logger.error("Step failed", extra={"step": "parse", "asset": "census-pop"})
```

Output:
```
2026-03-17 16:56:51 | INFO    | pipeline.steps.parse | Processing started | workflow=uscis-forms
```

The `extra` fields `workflow`, `step`, and `asset` are promoted to dedicated indexed columns in the database for fast filtering.

## Environment Variables

### Ingestion Processing (.env)

```env
# Database
DATABASE_URL=postgresql://pipeline:pipeline@localhost:5432/pipeline

# Logging
PIPELINE_LOG_DB=true              # Persist logs to PostgreSQL
PIPELINE_LOG_LEVEL=INFO           # Minimum log level (DEBUG, INFO, WARNING, ERROR)
PIPELINE_LOG_DIR=./logs           # Optional: directory for log files

# MinIO
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=gov-data-lake

# Pipeline
MANIFESTS_PATH=./manifests

# API Keys
# Claude Vision (PDF/image parsing)
CLAUDE_VISION_KEY=your_claude_vision_key_here
CLAUDE_VISION_URL=https://usafacts-poc-resource.services.ai.azure.com/anthropic
ANTHROPIC_API_KEY=your_api_key_here
ANTHROPIC_BASE_URL=https://usafacts-poc-resource.services.ai.azure.com/anthropic
```

### Backend (.env)

```env
DATABASE_URL=postgresql://pipeline:pipeline@localhost:5432/pipeline
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=gov-data-lake
CORS_ORIGINS=http://localhost:5173,http://localhost:3000
ANTHROPIC_API_KEY=your_api_key_here
ANTHROPIC_BASE_URL=https://usafacts-poc-resource.services.ai.azure.com/anthropic
```

### Frontend (.env)

```env
VITE_API_URL=http://localhost:8000/api
```

## Storage Zones

MinIO object storage organization:

- **landing-zone/**: Raw acquired data
- **parsed-zone/**: Structured JSON documents
- **enrichment-zone/**: Enriched documents with semantic context
- **ready-zone/**: Production-ready data

## Docker Services

| Service | Port | Description |
|---------|------|-------------|
| **postgres** | 5432 | PostgreSQL 16 — metadata catalog + pipeline logs |
| **minio** | 9000, 9001 | MinIO object storage + web console |
| **backend** | 8000 | FastAPI REST API |
| **frontend** | 3000 | React UI (Nginx) |
| **jupyter** | 8888 | JupyterLab notebooks |
| **mcp-embedding** | 8080 | MCP embedding server |

## Provider Plugin Architecture

New capabilities are added by dropping a provider folder under `providers/`:

```
providers/
  mda_ingestion_provider/          # In-process: wraps existing steps
    mda_plugin.yaml
    engines/python_v0/
      acquisition/v1/default_acquisition.py
      parse/v1/default_parse.py
      enrichment/v1/default_enrichment.py

  mda_semantics_provider/          # Network: calls MCP servers
    mda_plugin.yaml
    engines/mcp_v0/
      embed/v1/embed_capability.py
```

No core code changes required to add a provider.

## Development

### Running Tests

```bash
# Ingestion processing tests
cd ingestion-processing && uv run pytest

# Backend tests
cd backend && uv run pytest
```

### Code Quality

```bash
# Lint and format
cd ingestion-processing && uv run ruff check --fix
cd backend && uv run ruff check --fix
cd frontend && npm run lint
```

## License

Private - USAFacts
