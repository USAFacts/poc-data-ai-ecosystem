"""Metadata assistant for LLM-powered metadata curation.

This module provides tools for data analysts to curate table and column
metadata using LLM-powered suggestions and persist them to a catalog.
"""

from metadata_assistant.models import (
    TableMetadata,
    ColumnMetadata,
    TableRelationship,
    ValidationResult,
)
from metadata_assistant.assistant import MetadataAssistant
from metadata_assistant.catalog import CatalogClient

__all__ = [
    "MetadataAssistant",
    "TableMetadata",
    "ColumnMetadata",
    "TableRelationship",
    "ValidationResult",
    "CatalogClient",
]
