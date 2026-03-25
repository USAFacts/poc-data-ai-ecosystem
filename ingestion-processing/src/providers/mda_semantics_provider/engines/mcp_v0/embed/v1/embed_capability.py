"""Embed Capability — calls a containerized MCP server for embeddings.

Ported from model_D/mda_semantics_provider/engines/mcp_v0/embed/v1/embed_capability.py.

This capability demonstrates MDA's infrastructure-agnostic design:
it implements the same CapabilityInterface as any in-process capability,
but under the hood it calls an external MCP server running in Docker.

The interpreter and orchestrator see no difference.
"""

import asyncio
import json
import os
from typing import Any

from mda.capability.interface import CapabilityInterface
from runtime.context import ExecutionContext


class EmbedCapability(CapabilityInterface):
    """Calls an MCP embedding server to produce vector embeddings.

    Params:
        input_text: Text to embed.
        mcp_server_url: URL of the MCP embedding server (overridable via MCP_SERVER_URL env var).
    """

    def __init__(self, context: ExecutionContext, params: dict[str, Any]) -> None:
        super().__init__(context, params)

    def validate_params(self) -> bool:
        """Require input_text and mcp_server_url."""
        return "input_text" in self.params and "mcp_server_url" in self.params

    def execute(self) -> dict[str, Any]:
        """Call MCP server and return embedding result."""
        text = self.params["input_text"]

        # Manifest declares the default URL; environment can override
        # for deployment-specific service discovery (the PDM layer).
        mcp_url = os.environ.get("MCP_SERVER_URL", self.params["mcp_server_url"])

        self.log(f"Calling MCP embedding server at {mcp_url}")

        embedding = asyncio.run(self._call_mcp_embed(text, mcp_url))

        self.log(f"Received {embedding['dimensions']}-dim vector (model: {embedding['model']})")

        return {
            "status": "success",
            "embedding": embedding,
        }

    async def _call_mcp_embed(self, text: str, url: str) -> dict:
        """Call the MCP embedding server via Streamable HTTP.

        Args:
            text: Input text to embed.
            url: MCP server URL.

        Returns:
            Embedding result dict with vector, dimensions, model.
        """
        from mcp import ClientSession
        from mcp.client.streamable_http import streamablehttp_client

        async with streamablehttp_client(url) as (read_stream, write_stream, _):
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()
                result = await session.call_tool("embed", {"text": text})
                return json.loads(result.content[0].text)
