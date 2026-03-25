"""Parse step implementation."""

import json
from datetime import datetime, timezone
from typing import Any

from runtime.context import ExecutionContext
from steps.base import Step, StepResult, StepStatus
from steps.parse.parsers import (
    Parser,
    ParserError,
    get_parser,
    get_parser_for_format,
)
from storage.naming import LANDING_ZONE, PARSED_ZONE

# Schema identifier for parsed documents
PARSED_DOCUMENT_SCHEMA = "https://usafacts.org/schemas/parsed-document/v1"


class ParseStep(Step):
    """Step that parses documents and extracts structured content.

    Reads documents from landing-zone, parses them using the appropriate
    parser (Claude Vision, basic, etc.), and stores structured JSON in parsed-zone.

    The output follows an industry-standard schema combining:
    - Dublin Core (ISO 15836) metadata elements
    - Structured content (sections, tables, key-values)
    - Extraction metadata

    Configuration options:
    - parser: Parser type to use (vision, basic, auto)
    - parser_config: Parser-specific configuration
    """

    step_type = "parse"

    def __init__(self, name: str, config: dict[str, Any] | None = None) -> None:
        """Initialize parse step.

        Args:
            name: Step name
            config: Step configuration
        """
        super().__init__(name, config)
        self._parser_cache: dict[str, Parser] = {}

    def _get_parser(self, parser_type: str, format: str) -> Parser:
        """Get or create a parser instance.

        Args:
            parser_type: Requested parser type (or "auto")
            format: File format for auto-detection

        Returns:
            Parser instance

        Raises:
            ValueError: If no suitable parser found
        """
        # Get parser config
        parser_config = self.config.get("parser_config", {})

        # Use cache for efficiency
        cache_key = parser_type
        if cache_key not in self._parser_cache:
            parser = get_parser(parser_type, **parser_config)
            if parser is None:
                raise ValueError(f"Unknown parser type: {parser_type}")
            self._parser_cache[cache_key] = parser

        return self._parser_cache[cache_key]

    def validate_config(self) -> list[str]:
        """Validate step configuration.

        Returns:
            List of validation error messages
        """
        errors = []

        parser_type = self.config.get("parser", "auto")
        if parser_type not in ("auto", "vision", "basic"):
            errors.append(f"Unknown parser type: {parser_type}")

        return errors

    def execute(self, context: ExecutionContext) -> StepResult:
        """Execute the parse step.

        Reads document from landing-zone, parses it, and stores
        structured JSON in parsed-zone.

        Args:
            context: Execution context with asset, storage, etc.

        Returns:
            StepResult with status and output
        """
        started_at = datetime.now(timezone.utc)

        try:
            # Get acquisition output (source document info)
            acq_output = context.get_step_output("acquire")
            if not acq_output:
                return StepResult(
                    status=StepStatus.FAILED,
                    started_at=started_at,
                    completed_at=datetime.now(timezone.utc),
                    error="No acquisition output found. Parse step requires acquisition step to run first.",
                )

            # Extract info from acquisition output
            source_path = acq_output.get("object_path", "")
            source_url = acq_output.get("source_url", "")
            file_format = acq_output.get("format", "")
            checksum = acq_output.get("checksum", "")
            bytes_stored = acq_output.get("bytes_stored", 0)

            # Read source document from storage
            storage = context.storage
            data = storage.get_object(source_path)

            # Get parser
            parser_type = self.config.get("parser", "auto")
            parser = self._get_parser(parser_type, file_format)

            # Parse document
            filename = f"{context.asset.metadata.name}.{file_format}"
            parse_result = parser.parse(data, filename)

            # Compute quality metrics
            quality_metrics = parse_result.compute_quality(bytes_stored)

            # Build parsed document following schema
            parsed_document = self._build_parsed_document(
                context=context,
                parse_result=parse_result,
                source_path=source_path,
                source_url=source_url,
                file_format=file_format,
                checksum=checksum,
                bytes_stored=bytes_stored,
                quality_metrics=quality_metrics,
            )

            # Store parsed document in parsed-zone
            output_path = self._store_parsed_document(
                context=context,
                parsed_document=parsed_document,
            )

            # Build output including quality scores
            output = {
                "object_path": output_path,
                "source_path": source_path,
                "parser": parse_result.parser,
                "page_count": parse_result.page_count,
                "section_count": len(parse_result.sections),
                "table_count": len(parse_result.tables),
                "zone": PARSED_ZONE,
                "quality": {
                    "overall_score": round(quality_metrics.overall_score, 1),
                    "extraction_score": round(quality_metrics.extraction_score, 1),
                    "structural_score": round(quality_metrics.structural_score, 1),
                    "ai_readiness_score": round(quality_metrics.ai_readiness_score, 1),
                    "estimated_tokens": quality_metrics.content.estimated_tokens,
                },
            }

            return StepResult(
                status=StepStatus.SUCCESS,
                started_at=started_at,
                completed_at=datetime.now(timezone.utc),
                output=output,
            )

        except ParserError as e:
            return StepResult(
                status=StepStatus.FAILED,
                started_at=started_at,
                completed_at=datetime.now(timezone.utc),
                error=str(e),
            )
        except Exception as e:
            return StepResult(
                status=StepStatus.FAILED,
                started_at=started_at,
                completed_at=datetime.now(timezone.utc),
                error=f"Unexpected error: {e}",
            )

    def _build_parsed_document(
        self,
        context: ExecutionContext,
        parse_result: Any,
        source_path: str,
        source_url: str,
        file_format: str,
        checksum: str,
        bytes_stored: int,
        quality_metrics: Any = None,
    ) -> dict[str, Any]:
        """Build parsed document following the schema.

        Args:
            context: Execution context
            parse_result: Result from parser
            source_path: Path to source document in landing-zone
            source_url: Original URL where document was acquired
            file_format: File format
            checksum: Checksum of source document
            bytes_stored: Size of source document
            quality_metrics: Quality metrics for the extraction

        Returns:
            Parsed document dictionary
        """
        execution_time = context.execution_time
        agency_name = context.agency.metadata.name
        asset_name = context.asset.metadata.name

        # Build output path for reference
        datestamp = execution_time.strftime("%Y-%m-%d")
        timestamp = execution_time.strftime("%H%M%S")
        output_path = f"{PARSED_ZONE}/{agency_name}/{asset_name}/{datestamp}/{timestamp}/{asset_name}.json"

        # Determine document type from format
        type_mapping = {
            "xlsx": "Spreadsheet",
            "xls": "Spreadsheet",
            "csv": "Dataset",
            "json": "Dataset",
            "pdf": "Document",
            "docx": "Document",
            "doc": "Document",
            "pptx": "Presentation",
            "ppt": "Presentation",
        }

        document = {
            "$schema": PARSED_DOCUMENT_SCHEMA,
            "metadata": {
                "identifier": f"{agency_name}/{asset_name}/{datestamp}/{timestamp}",
                "title": parse_result.title or asset_name,
                "publisher": context.agency.spec.full_name,
                "date": execution_time.isoformat(),
                "type": type_mapping.get(file_format, "Document"),
                "format": file_format,
                "language": parse_result.language or "en",
                "subject": list(context.asset.metadata.labels.get("domain", "").split(",")) if context.asset.metadata.labels.get("domain") else [],
            },
            "source": {
                "originalUrl": source_url,
                "storageUrl": source_path,
                "parsedStorageUrl": output_path,
                "filename": f"{asset_name}.{file_format}",
                "fileSize": bytes_stored,
                "checksum": checksum,
                "mimeType": self._get_mime_type(file_format),
                "pageCount": parse_result.page_count,
                "agency": agency_name,
                "asset": asset_name,
                "workflow": context.plan.workflow_name,
                "run_id": context.master_utid or context.run_id,
                "version": f"{datestamp}/{timestamp}",
            },
            "content": parse_result.to_content_dict(),
            "extraction": parse_result.to_extraction_dict(),
        }

        # Add quality metrics if available
        if quality_metrics:
            document["quality"] = quality_metrics.to_dict()

        return document

    def _store_parsed_document(
        self,
        context: ExecutionContext,
        parsed_document: dict[str, Any],
    ) -> str:
        """Store parsed document in parsed-zone.

        Args:
            context: Execution context
            parsed_document: Parsed document to store

        Returns:
            Object path where document was stored
        """
        storage = context.storage
        execution_time = context.execution_time
        agency_name = context.agency.metadata.name
        asset_name = context.asset.metadata.name

        # Build path
        datestamp = execution_time.strftime("%Y-%m-%d")
        timestamp = execution_time.strftime("%H%M%S")
        object_path = f"{PARSED_ZONE}/{agency_name}/{asset_name}/{datestamp}/{timestamp}/{asset_name}.json"

        # Serialize to JSON
        json_data = json.dumps(parsed_document, indent=2, ensure_ascii=False).encode("utf-8")

        # Build metadata
        metadata = {
            "schema": PARSED_DOCUMENT_SCHEMA,
            "source_path": parsed_document["source"]["storageUrl"],
            "parser": parsed_document["extraction"]["parser"],
            "workflow": context.plan.workflow_name,
            "run_id": context.master_utid or context.run_id,
            "zone": PARSED_ZONE,
        }

        # Upload to storage
        storage.put_object(
            object_name=object_path,
            data=json_data,
            content_type="application/json",
            metadata=metadata,
        )

        return object_path

    def _get_mime_type(self, format: str) -> str:
        """Get MIME type for a format."""
        mime_types = {
            "csv": "text/csv",
            "json": "application/json",
            "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "xls": "application/vnd.ms-excel",
            "pdf": "application/pdf",
            "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "doc": "application/msword",
            "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            "ppt": "application/vnd.ms-powerpoint",
            "txt": "text/plain",
            "xml": "application/xml",
        }
        return mime_types.get(format, "application/octet-stream")
