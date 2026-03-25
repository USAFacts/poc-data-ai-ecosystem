"""Base classes for document chunkers."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


class ChunkerError(Exception):
    """Raised when a chunker encounters an unrecoverable error."""

    pass


@dataclass
class Chunk:
    """A single chunk from a parsed document."""

    chunk_id: str
    parent_chunk_id: str | None
    document_id: str
    level: str  # "document", "section", "table"
    sequence: int
    text: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "chunk_id": self.chunk_id,
            "parent_chunk_id": self.parent_chunk_id,
            "document_id": self.document_id,
            "level": self.level,
            "sequence": self.sequence,
            "text": self.text,
            "metadata": self.metadata,
        }


@dataclass
class ChunkResult:
    """Result of chunking a document."""

    chunks: list[Chunk]
    document_id: str
    total_chunks: int = 0

    def __post_init__(self) -> None:
        self.total_chunks = len(self.chunks)


class Chunker(ABC):
    """Abstract base class for document chunkers."""

    @abstractmethod
    def chunk(self, parsed_document: dict[str, Any]) -> ChunkResult:
        """Split a parsed document into chunks.

        Args:
            parsed_document: Parsed document dict following the parsed-document/v1 schema.

        Returns:
            ChunkResult containing the hierarchical chunks.
        """
        pass
