"""Default Parse Capability — wraps ParseStep via CapabilityAdapter.

This is the MDA entry point for the existing parse step.
The DefaultResolver resolves 'parse/v1/default_parse' to this module.
"""

from typing import Any

from mda.capability.adapter import CapabilityAdapter
from runtime.context import ExecutionContext
from steps.parse.step import ParseStep


class DefaultParseCapability(CapabilityAdapter):
    """MDA capability wrapping the legacy ParseStep."""

    def __init__(self, context: ExecutionContext, params: dict[str, Any]) -> None:
        super().__init__(context, params, step_class=ParseStep)
