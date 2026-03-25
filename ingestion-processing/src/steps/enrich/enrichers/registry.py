"""Enricher type registry."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from steps.enrich.enrichers.base import Enricher

# Registry of enricher type -> enricher class
_ENRICHER_REGISTRY: dict[str, type["Enricher"]] = {}


def register_enricher(enricher_type: str, enricher_class: type["Enricher"]) -> None:
    """Register an enricher class for an enricher type.

    Args:
        enricher_type: Type identifier (e.g., "llm", "basic", "auto")
        enricher_class: Enricher class to instantiate for this type
    """
    _ENRICHER_REGISTRY[enricher_type] = enricher_class


def get_enricher(enricher_type: str, **kwargs) -> "Enricher | None":
    """Get an instance of an enricher for the given type.

    Args:
        enricher_type: Type identifier
        **kwargs: Arguments to pass to enricher constructor

    Returns:
        Enricher instance or None if type not found
    """
    if not _ENRICHER_REGISTRY:
        _load_builtin_enrichers()

    enricher_class = _ENRICHER_REGISTRY.get(enricher_type)
    if enricher_class is None:
        return None

    return enricher_class(**kwargs)


def get_enricher_class(enricher_type: str) -> type["Enricher"] | None:
    """Get the enricher class for a type without instantiating.

    Args:
        enricher_type: Type identifier

    Returns:
        Enricher class or None if not found
    """
    if not _ENRICHER_REGISTRY:
        _load_builtin_enrichers()

    return _ENRICHER_REGISTRY.get(enricher_type)


def get_registered_enrichers() -> list[str]:
    """Get list of registered enricher types."""
    if not _ENRICHER_REGISTRY:
        _load_builtin_enrichers()
    return list(_ENRICHER_REGISTRY.keys())


def _load_builtin_enrichers() -> None:
    """Load built-in enricher types."""
    from steps.enrich.enrichers.basic import BasicEnricher
    from steps.enrich.enrichers.llm import LLMEnricher
    from steps.enrich.enrichers.auto import AutoEnricher

    register_enricher("basic", BasicEnricher)
    register_enricher("llm", LLMEnricher)
    register_enricher("auto", AutoEnricher)
