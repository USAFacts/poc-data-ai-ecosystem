"""Default Chunk Capability — wraps ChunkStep via CapabilityAdapter.

The DefaultResolver resolves 'chunk/v1/default_chunk' to this module.
"""

from typing import Any

from mda.capability.adapter import CapabilityAdapter
from runtime.context import ExecutionContext
from steps.chunk.step import ChunkStep


class DefaultChunkCapability(CapabilityAdapter):
    """MDA capability wrapping the ChunkStep."""

    def __init__(self, context: ExecutionContext, params: dict[str, Any]) -> None:
        super().__init__(context, params, step_class=ChunkStep)
