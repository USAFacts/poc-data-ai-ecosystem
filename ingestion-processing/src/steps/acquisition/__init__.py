"""Data acquisition step."""

from steps.acquisition.step import AcquisitionStep
from steps.acquisition.temporal import (
    TemporalContext,
    resolve_temporal_url,
    resolve_temporal_context,
    get_previous_period,
)
from steps.acquisition.connectors import (
    Connector,
    ConnectorError,
    ConnectorResult,
    HttpConnector,
    ApiConnector,
    register_connector,
    get_connector,
    get_registered_connectors,
)

__all__ = [
    # Step
    "AcquisitionStep",
    # Temporal utilities
    "TemporalContext",
    "resolve_temporal_url",
    "resolve_temporal_context",
    "get_previous_period",
    # Connectors
    "Connector",
    "ConnectorError",
    "ConnectorResult",
    "HttpConnector",
    "ApiConnector",
    "register_connector",
    "get_connector",
    "get_registered_connectors",
]
