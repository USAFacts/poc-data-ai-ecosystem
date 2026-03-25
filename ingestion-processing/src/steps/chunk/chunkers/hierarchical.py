"""Hierarchical chunker — splits documents into document, section, and table chunks."""

import hashlib
from typing import Any

from steps.chunk.chunkers.base import Chunk, ChunkerError, ChunkResult, Chunker


def _make_chunk_id(document_id: str, level: str, source_id: str) -> str:
    """Generate a deterministic chunk ID."""
    raw = f"{document_id}:{level}:{source_id}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


class HierarchicalChunker(Chunker):
    """Splits a parsed document into a three-level hierarchy.

    Levels:
        - document: one chunk per document (title + leading content)
        - section: one chunk per content.sections[] entry
        - table: one chunk per content.tables[] entry (markdown representation)

    Section and table chunks reference the document chunk as their parent.
    """

    MAX_DOC_CHARS = 2000

    def chunk(self, parsed_document: dict[str, Any]) -> ChunkResult:
        """Split a parsed document into hierarchical chunks."""
        metadata = parsed_document.get("metadata", {})
        content = parsed_document.get("content", {})
        source = parsed_document.get("source", {})

        document_id = metadata.get("identifier", "")
        if not document_id:
            raise ChunkerError("Parsed document missing metadata.identifier")

        chunks: list[Chunk] = []
        seq = 0

        # --- Document-level chunk ---
        doc_chunk_id = _make_chunk_id(document_id, "document", "root")
        title = metadata.get("title", "")
        plain_text = content.get("plainText", "")
        doc_text = f"{title}\n\n{plain_text[:self.MAX_DOC_CHARS]}" if plain_text else title

        chunks.append(Chunk(
            chunk_id=doc_chunk_id,
            parent_chunk_id=None,
            document_id=document_id,
            level="document",
            sequence=seq,
            text=doc_text.strip(),
            metadata={
                "title": title,
                "agency": source.get("agency", ""),
                "asset": source.get("asset", ""),
                "level_depth": 0,
                "token_estimate": len(doc_text.split()),
            },
        ))
        seq += 1

        # --- Section-level chunks ---
        for section in content.get("sections", []):
            section_id = section.get("id", f"section-{seq}")
            section_text = section.get("content", "")
            if not section_text.strip():
                continue

            chunks.append(Chunk(
                chunk_id=_make_chunk_id(document_id, "section", section_id),
                parent_chunk_id=doc_chunk_id,
                document_id=document_id,
                level="section",
                sequence=seq,
                text=section_text.strip(),
                metadata={
                    "title": section.get("title", ""),
                    "section_id": section_id,
                    "page_number": section.get("page_number"),
                    "sheet_name": section.get("sheet_name"),
                    "level_depth": 1,
                    "token_estimate": len(section_text.split()),
                },
            ))
            seq += 1

        # --- Table-level chunks ---
        for table in content.get("tables", []):
            table_id = table.get("id", f"table-{seq}")
            table_text = table.get("markdown", "")
            if not table_text.strip():
                # Fallback: build markdown from headers + rows
                headers = table.get("headers", [])
                rows = table.get("rows", [])
                if headers:
                    table_text = " | ".join(headers) + "\n"
                    for row in rows[:20]:  # Cap rows to avoid giant chunks
                        table_text += " | ".join(str(c) for c in row) + "\n"

            if not table_text.strip():
                continue

            chunks.append(Chunk(
                chunk_id=_make_chunk_id(document_id, "table", table_id),
                parent_chunk_id=doc_chunk_id,
                document_id=document_id,
                level="table",
                sequence=seq,
                text=table_text.strip(),
                metadata={
                    "title": table.get("title", ""),
                    "table_id": table_id,
                    "headers": table.get("headers", []),
                    "page_number": table.get("page_number"),
                    "sheet_name": table.get("sheet_name"),
                    "level_depth": 1,
                    "token_estimate": len(table_text.split()),
                },
            ))
            seq += 1

        return ChunkResult(chunks=chunks, document_id=document_id)
