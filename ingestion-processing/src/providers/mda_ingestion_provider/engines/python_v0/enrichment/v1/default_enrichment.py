"""Default Enrichment Capability — wraps EnrichmentStep via CapabilityAdapter.

This is the MDA entry point for the existing enrichment step.
The DefaultResolver resolves 'enrichment/v1/default_enrichment' to this module.
"""

from typing import Any

from mda.capability.adapter import CapabilityAdapter
from runtime.context import ExecutionContext
from steps.enrich.step import EnrichmentStep


class DefaultEnrichmentCapability(CapabilityAdapter):
    """MDA capability wrapping the legacy EnrichmentStep."""

    def __init__(self, context: ExecutionContext, params: dict[str, Any]) -> None:
        super().__init__(context, params, step_class=EnrichmentStep)
