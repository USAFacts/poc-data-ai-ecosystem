"""MCP Embedding Server — Containerized tool server for MDA.

Ported from model_D/mcp_servers/embedding_server/server.py.

Exposes an `embed` tool via Streamable HTTP transport.
Uses deterministic hash-based vectors (mock model) for the POC.

Run:
    python server.py
    # or: mcp run server.py --transport streamable-http
"""

import hashlib
import json

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("MDA Embedding Server", host="0.0.0.0", port=8000)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
EMBEDDING_DIM = 8          # 8-float vector (SHA-256 -> 32 bytes -> 8 floats)
MODEL_ID = "mock-hash-v1"  # identifier for the mock model


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------
@mcp.tool()
def embed(text: str) -> str:
    """Generate a deterministic embedding vector for the given text.

    The vector is derived from a SHA-256 hash, producing reproducible
    results for the same input. This is a mock model for POC purposes;
    swap with a real model (sentence-transformers, OpenAI, etc.) later.

    Args:
        text: The input text to embed.

    Returns:
        JSON string with vector, dimensions, and model metadata.
    """
    digest = hashlib.sha256(text.encode("utf-8")).digest()

    # Convert each 4-byte chunk to an unsigned int, then map to [-1.0, 1.0]
    raw_vector = []
    for i in range(EMBEDDING_DIM):
        chunk = digest[i * 4 : (i + 1) * 4]
        uint_val = int.from_bytes(chunk, "little", signed=False)
        # Map [0, 2^32) -> [-1.0, 1.0)
        raw_vector.append((uint_val / (2**31)) - 1.0)

    # L2-normalize
    norm = sum(x ** 2 for x in raw_vector) ** 0.5
    if norm == 0:
        norm = 1.0
    vector = [round(x / norm, 6) for x in raw_vector]

    return json.dumps({
        "vector": vector,
        "dimensions": EMBEDDING_DIM,
        "model": MODEL_ID,
        "input_hash": hashlib.sha256(text.encode()).hexdigest()[:12],
    })


@mcp.tool()
def list_models() -> str:
    """List available embedding models."""
    return json.dumps({
        "models": [
            {"id": MODEL_ID, "dimensions": EMBEDDING_DIM, "type": "mock-hash"}
        ]
    })


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    mcp.run(transport="streamable-http")
