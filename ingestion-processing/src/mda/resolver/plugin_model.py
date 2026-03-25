"""Pydantic model for mda_plugin.yaml validation.

Ported from model_D/mda/provider/interpreter/v1/plugin_model.py.
"""

from typing import Optional

from pydantic import BaseModel, Field


class ConnectorParams(BaseModel):
    """Connector parameters."""

    store_path: str = Field(..., description="Path to the store relative to model root")


class ConnectorConfig(BaseModel):
    """Connector configuration."""

    module_path: str = Field(..., description="Module path for the connector")
    params: ConnectorParams


class StoreConfig(BaseModel):
    """Store configuration."""

    connector: ConnectorConfig


class StoresConfig(BaseModel):
    """All stores configuration."""

    manifest_store: StoreConfig
    evidence_store: Optional[StoreConfig] = None


class PluginConfig(BaseModel):
    """Root model for mda_plugin.yaml.

    Validates the provider plugin configuration structure.
    """

    stores: StoresConfig
    resolver: Optional[dict[str, str]] = Field(
        default=None,
        description="Capability URN to module path mapping",
    )

    def get_manifest_store_path(self) -> str:
        """Get manifest store path from config."""
        return self.stores.manifest_store.connector.params.store_path

    def get_evidence_store_path(self) -> Optional[str]:
        """Get evidence store path from config if exists."""
        if self.stores.evidence_store:
            return self.stores.evidence_store.connector.params.store_path
        return None

    def get_resolver_mapping(self, capability_urn: str) -> Optional[str]:
        """Look up capability module path from resolver mapping."""
        if self.resolver:
            return self.resolver.get(capability_urn)
        return None
