"""Parser type registry."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from steps.parse.parsers.base import Parser

# Registry of parser type -> parser class
_PARSER_REGISTRY: dict[str, type["Parser"]] = {}


def register_parser(parser_type: str, parser_class: type["Parser"]) -> None:
    """Register a parser class for a parser type.

    Args:
        parser_type: Type identifier (e.g., "vision", "basic", "auto")
        parser_class: Parser class to instantiate for this type
    """
    _PARSER_REGISTRY[parser_type] = parser_class


def get_parser(parser_type: str, **kwargs) -> "Parser | None":
    """Get an instance of a parser for the given type.

    Args:
        parser_type: Type identifier
        **kwargs: Arguments to pass to parser constructor

    Returns:
        Parser instance or None if type not found
    """
    if not _PARSER_REGISTRY:
        _load_builtin_parsers()

    parser_class = _PARSER_REGISTRY.get(parser_type)
    if parser_class is None:
        return None

    return parser_class(**kwargs)


def get_parser_class(parser_type: str) -> type["Parser"] | None:
    """Get the parser class for a type without instantiating.

    Args:
        parser_type: Type identifier

    Returns:
        Parser class or None if not found
    """
    if not _PARSER_REGISTRY:
        _load_builtin_parsers()

    return _PARSER_REGISTRY.get(parser_type)


def get_registered_parsers() -> list[str]:
    """Get list of registered parser types."""
    if not _PARSER_REGISTRY:
        _load_builtin_parsers()
    return list(_PARSER_REGISTRY.keys())


def get_parser_for_format(format: str, **kwargs) -> "Parser | None":
    """Get a parser that supports the given format.

    Args:
        format: File format (e.g., "xlsx", "pdf", "csv")
        **kwargs: Arguments to pass to parser constructor

    Returns:
        Parser instance or None if no parser supports the format
    """
    if not _PARSER_REGISTRY:
        _load_builtin_parsers()

    for parser_class in _PARSER_REGISTRY.values():
        if format.lower() in [f.lower() for f in parser_class.supported_formats]:
            return parser_class(**kwargs)

    return None


def _load_builtin_parsers() -> None:
    """Load built-in parser types."""
    from steps.parse.parsers.basic import BasicParser
    from steps.parse.parsers.vision import VisionParser
    from steps.parse.parsers.auto import AutoParser

    register_parser("basic", BasicParser)
    register_parser("vision", VisionParser)
    register_parser("auto", AutoParser)
