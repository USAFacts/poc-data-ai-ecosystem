"""Data acquisition connectors.

Connectors are pluggable components that handle data acquisition from
different source types (HTTP, API, SFTP, S3, etc.).
"""

from steps.acquisition.connectors.base import (
    Connector,
    ConnectorError,
    ConnectorResult,
)
from steps.acquisition.connectors.http import HttpConnector
from steps.acquisition.connectors.api import ApiConnector
from steps.acquisition.connectors.registry import (
    register_connector,
    get_connector,
    get_registered_connectors,
)

__all__ = [
    "Connector",
    "ConnectorError",
    "ConnectorResult",
    "HttpConnector",
    "ApiConnector",
    "register_connector",
    "get_connector",
    "get_registered_connectors",
]
