"""Default Sync Capability — wraps SyncStep via CapabilityAdapter.

The DefaultResolver resolves 'sync/v1/default_sync' to this module.
"""

from typing import Any

from mda.capability.adapter import CapabilityAdapter
from runtime.context import ExecutionContext
from steps.sync.step import SyncStep


class DefaultSyncCapability(CapabilityAdapter):
    """MDA capability wrapping the SyncStep."""

    def __init__(self, context: ExecutionContext, params: dict[str, Any]) -> None:
        super().__init__(context, params, step_class=SyncStep)
