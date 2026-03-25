"""Default Acquisition Capability — wraps AcquisitionStep via CapabilityAdapter.

This is the MDA entry point for the existing acquisition step.
The DefaultResolver resolves 'acquisition/v1/default_acquisition' to this module.
"""

from typing import Any

from mda.capability.adapter import CapabilityAdapter
from runtime.context import ExecutionContext
from steps.acquisition.step import AcquisitionStep


class DefaultAcquisitionCapability(CapabilityAdapter):
    """MDA capability wrapping the legacy AcquisitionStep."""

    def __init__(self, context: ExecutionContext, params: dict[str, Any]) -> None:
        super().__init__(context, params, step_class=AcquisitionStep)
