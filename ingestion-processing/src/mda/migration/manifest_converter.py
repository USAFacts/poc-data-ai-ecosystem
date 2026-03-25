"""Manifest Converter — converts pipeline/v1 manifests to standard/1.0 format.

Converts a set of pipeline/v1 manifests (agency + asset + workflow)
into a single standard/1.0 manifest that the StandardParser can process.
"""

from typing import Any

import yaml

from control.models import Agency, Asset, Workflow


# Step type to component path mapping
_STEP_TYPE_MAP = {
    "acquisition": "acquisition/v1/default_acquisition",
    "parse": "parse/v1/default_parse",
    "enrichment": "enrichment/v1/default_enrichment",
}


def convert_to_standard(
    agency: Agency,
    asset: Asset,
    workflow: Workflow,
) -> dict[str, Any]:
    """Convert pipeline/v1 manifests to a standard/1.0 manifest dict.

    Args:
        agency: Agency model.
        asset: Asset model.
        workflow: Workflow model.

    Returns:
        Dict in standard/1.0 format.
    """
    # Build identity block
    identity = {
        "name": asset.metadata.name,
        "layer": "curation",
        "domain": agency.metadata.labels.get("category", "general"),
        "agency": agency.metadata.name,
        "owner": "DAM",
    }

    # Build evolution block
    evolution = {
        "manifest_id": workflow.metadata.name,
        "manifest_version": "1.0.0",
        "provider": "mda_ingestion_provider",
        "engine": "python_v0",
    }

    # Build steps
    steps = []
    for step in workflow.spec.steps:
        component_path = _STEP_TYPE_MAP.get(step.type)
        if not component_path:
            raise ValueError(f"Unknown step type: {step.type}")

        params = dict(step.config)
        params["step_name"] = step.name

        steps.append({
            "step": step.name,
            "component": {
                "path": component_path,
                "params": params,
            },
        })

    return {
        "schema": "standard/1.0",
        "identity": identity,
        "evolution": evolution,
        "steps": steps,
    }


def convert_to_yaml(
    agency: Agency,
    asset: Asset,
    workflow: Workflow,
) -> str:
    """Convert pipeline/v1 manifests to a standard/1.0 YAML string.

    Args:
        agency: Agency model.
        asset: Asset model.
        workflow: Workflow model.

    Returns:
        YAML string in standard/1.0 format.
    """
    manifest = convert_to_standard(agency, asset, workflow)
    return yaml.dump(manifest, default_flow_style=False, sort_keys=False)
