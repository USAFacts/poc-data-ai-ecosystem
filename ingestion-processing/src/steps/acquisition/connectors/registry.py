"""Connector type registry."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from steps.acquisition.connectors.base import Connector

# Registry of connector type -> connector class
_CONNECTOR_REGISTRY: dict[str, type["Connector"]] = {}


def register_connector(connector_type: str, connector_class: type["Connector"]) -> None:
    """Register a connector class for a connector type.

    Args:
        connector_type: Type identifier (e.g., "http", "api", "sftp")
        connector_class: Connector class to instantiate for this type
    """
    _CONNECTOR_REGISTRY[connector_type] = connector_class


def get_connector(connector_type: str, **kwargs) -> "Connector | None":
    """Get an instance of a connector for the given type.

    Args:
        connector_type: Type identifier
        **kwargs: Arguments to pass to connector constructor

    Returns:
        Connector instance or None if type not found
    """
    if not _CONNECTOR_REGISTRY:
        _load_builtin_connectors()

    connector_class = _CONNECTOR_REGISTRY.get(connector_type)
    if connector_class is None:
        return None

    return connector_class(**kwargs)


def get_connector_class(connector_type: str) -> type["Connector"] | None:
    """Get the connector class for a type without instantiating.

    Args:
        connector_type: Type identifier

    Returns:
        Connector class or None if not found
    """
    if not _CONNECTOR_REGISTRY:
        _load_builtin_connectors()

    return _CONNECTOR_REGISTRY.get(connector_type)


def get_registered_connectors() -> list[str]:
    """Get list of registered connector types."""
    if not _CONNECTOR_REGISTRY:
        _load_builtin_connectors()
    return list(_CONNECTOR_REGISTRY.keys())


def _load_builtin_connectors() -> None:
    """Load built-in connector types."""
    from steps.acquisition.connectors.http import HttpConnector
    from steps.acquisition.connectors.api import ApiConnector

    register_connector("http", HttpConnector)
    register_connector("api", ApiConnector)
