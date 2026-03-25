"""DefaultResolver — resolves capability component paths to Python classes.

Ported from model_D/mda/provider/resolver/v1/default_resolver.py.

Resolution order:
1. Explicit mapping in mda_plugin.yaml resolver section
2. Convention: {provider}.engines.{engine}.{component_path}
3. Fallback: {provider}.{component_path}
"""

import importlib
import inspect
from typing import Any

from mda.resolver.plugin_model import PluginConfig


class DefaultResolver:
    """Resolves capability component paths to capability classes.

    Given a provider name, engine name, and component path, the resolver
    finds the Python class that implements the capability.
    """

    def __init__(
        self,
        provider: str,
        engine: str | None = None,
        provider_config: PluginConfig | dict[str, Any] | None = None,
    ) -> None:
        """Initialize resolver.

        Args:
            provider: Provider package name (e.g., 'mda_ingestion_provider').
            engine: Engine name (e.g., 'python_v0').
            provider_config: PluginConfig model or raw dict with resolver mappings.
        """
        self.provider = provider
        self.engine = engine or ""

        # Support both PluginConfig model and legacy dict
        if isinstance(provider_config, PluginConfig):
            self.mappings = provider_config.resolver or {}
        else:
            self.mappings = (provider_config or {}).get("resolver", {})

    def resolve(self, component_path: str) -> type:
        """Resolve component path to capability class.

        Args:
            component_path: Path like 'acquisition/v1/default_acquisition'.

        Returns:
            The capability class.

        Raises:
            ValueError: If no capability found.
            ImportError: If module cannot be imported.
        """
        # Build capability URN for mapping lookup
        cap_urn = f"cap://{self.provider}:{self.engine}:{component_path}"

        # Normalize path separator: URNs use / but Python imports use .
        import_path = component_path.replace("/", ".")

        if cap_urn in self.mappings:
            module_path = (
                f"{self.provider}.{self.mappings[cap_urn].replace('/', '.')}"
            )
        elif self.engine:
            # Standard convention: provider.engines.engine.component_path
            module_path = f"{self.provider}.engines.{self.engine}.{import_path}"
        else:
            # No engine: provider.component_path
            module_path = f"{self.provider}.{import_path}"

        # Prefix with 'providers.' since providers live under src/providers/
        full_module_path = f"providers.{module_path}"

        # Import module and find capability class
        module = importlib.import_module(full_module_path)

        # Find class that has 'execute' method (capability contract)
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if hasattr(obj, "execute") and obj.__module__ == module.__name__:
                return obj

        # Fallback: look for module-level execute function
        if hasattr(module, "execute"):
            return module

        raise ValueError(f"No capability found in {full_module_path}")
