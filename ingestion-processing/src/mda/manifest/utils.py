"""Manifest utilities — schema-to-parser resolution.

Ported from model_D/mda/manifest/utils.py (get_parser function).
"""

import importlib
from pathlib import Path

import yaml

from mda.constants import PARSER_MAPPER_FILE


def get_parser(manifest_content: dict):
    """Get parser class based on manifest schema field.

    Resolution flow:
    1. Extract 'schema' from manifest (or 'apiVersion' for legacy manifests)
    2. Look up parser module path in mapper.yaml
    3. Import and return the parser class

    Args:
        manifest_content: Parsed manifest dict.

    Returns:
        Parser class (not instance).

    Raises:
        ValueError: If schema is missing or no parser found.
    """
    # Support both Model_D 'schema' field and legacy 'apiVersion' field
    schema = manifest_content.get("schema")
    if not schema:
        api_version = manifest_content.get("apiVersion")
        if api_version:
            schema = api_version
        else:
            raise ValueError("Manifest 'schema' (or 'apiVersion') field is missing.")

    # Load mapper
    mapper_path = Path(__file__).parent / PARSER_MAPPER_FILE
    with open(mapper_path, "r") as f:
        mapper = yaml.safe_load(f)

    # Find parser module path
    parser_module = mapper.get("mapper", {}).get(schema)
    if not parser_module:
        raise ValueError(f"No parser found for schema: {schema}")

    # Import parser module
    # "parsers/legacy_pipeline_parser" -> "mda.manifest.parsers.legacy_pipeline_parser"
    module_path = f"mda.manifest.{parser_module.replace('/', '.')}"
    module = importlib.import_module(module_path)

    # Find parser class (naming convention: *Parser, excluding ParserInterface)
    for name in dir(module):
        if name.endswith("Parser") and name != "ParserInterface":
            return getattr(module, name)

    raise ValueError(f"No parser class found in {module_path}")
