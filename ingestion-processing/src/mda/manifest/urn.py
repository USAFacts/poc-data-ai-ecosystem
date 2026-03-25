"""URN parsing and building for manifests and capabilities.

Ported from model_D/mda/manifest/utils.py (URN functions).

URN formats:
    Manifest: man://{layer}:{domain}:{manifest_path}[:{version}]
    Capability: cap://{provider}:{engine}:{component_path}
"""

from mda.constants import URN_SCHEME_MANIFEST, URN_SCHEME_CAPABILITY


def parse_manifest_urn(manifest_urn: str) -> dict:
    """Parse a manifest URN into its components.

    Args:
        manifest_urn: URN string like man://curation:demographics:census/pop:1.0.0

    Returns:
        Dict with keys: layer, domain, manifest_path, version.

    Raises:
        ValueError: If URN format is invalid.
    """
    prefix = f"{URN_SCHEME_MANIFEST}://"
    if not manifest_urn.startswith(prefix):
        raise ValueError(f"Invalid manifest URN: {manifest_urn}")

    parts = manifest_urn[len(prefix):].split(":")
    if len(parts) == 3:
        parts.append("latest")
    elif len(parts) != 4:
        raise ValueError(f"Invalid manifest URN format: {manifest_urn}")

    return {
        "layer": parts[0],
        "domain": parts[1],
        "manifest_path": parts[2],
        "version": parts[3],
    }


def build_manifest_urn(
    layer: str, domain: str, path: str, version: str = "latest"
) -> str:
    """Build a manifest URN from components.

    Args:
        layer: Architecture layer (e.g., 'curation', 'semantics').
        domain: Data domain (e.g., 'demographics', 'regulatory').
        path: Manifest path (e.g., 'census/census_population').
        version: Manifest version (default: 'latest').

    Returns:
        URN string like man://curation:demographics:census/pop:1.0.0
    """
    return f"{URN_SCHEME_MANIFEST}://{layer}:{domain}:{path}:{version}"


def build_capability_urn(provider: str, engine: str, path: str) -> str:
    """Build a capability URN from components.

    Args:
        provider: Provider name (e.g., 'mda_ingestion_provider').
        engine: Engine name (e.g., 'python_v0').
        path: Component path (e.g., 'acquisition/v1/default_acquisition').

    Returns:
        URN string like cap://mda_ingestion_provider:python_v0:acquisition/v1/default_acquisition
    """
    return f"{URN_SCHEME_CAPABILITY}://{provider}:{engine}:{path}"


def parse_capability_urn(capability_urn: str) -> dict:
    """Parse a capability URN into its components.

    Args:
        capability_urn: URN string like cap://provider:engine:path

    Returns:
        Dict with keys: provider, engine, component_path.

    Raises:
        ValueError: If URN format is invalid.
    """
    prefix = f"{URN_SCHEME_CAPABILITY}://"
    if not capability_urn.startswith(prefix):
        raise ValueError(f"Invalid capability URN: {capability_urn}")

    parts = capability_urn[len(prefix):].split(":", 2)
    if len(parts) != 3:
        raise ValueError(f"Invalid capability URN format: {capability_urn}")

    return {
        "provider": parts[0],
        "engine": parts[1],
        "component_path": parts[2],
    }
