"""Base connector interface for data acquisition."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date
from typing import Any


class ConnectorError(Exception):
    """Error during connector operation."""

    pass


@dataclass
class ConnectorResult:
    """Result from a connector fetch operation.

    Contains the raw data and metadata about the acquisition.
    """

    data: bytes
    metadata: dict[str, str] = field(default_factory=dict)

    @property
    def size(self) -> int:
        """Size of fetched data in bytes."""
        return len(self.data)

    @property
    def source_url(self) -> str:
        """Get the source URL from metadata."""
        return self.metadata.get("source_url", self.metadata.get("final_url", ""))


class Connector(ABC):
    """Abstract base class for data acquisition connectors.

    Connectors are responsible for fetching data from external sources.
    Each connector type handles a specific protocol or source type
    (HTTP, REST API, SFTP, S3, database, etc.).

    Subclasses must implement:
    - connector_type: Class attribute identifying the connector type
    - fetch(): Method to retrieve data from the source
    - validate_source(): Method to validate source configuration
    """

    # Connector type identifier (override in subclasses)
    connector_type: str = "base"

    def __init__(self, timeout: float = 300.0) -> None:
        """Initialize connector.

        Args:
            timeout: Operation timeout in seconds (default: 5 minutes)
        """
        self.timeout = timeout

    @abstractmethod
    def fetch(
        self,
        source: Any,
        reference_date: date | None = None,
    ) -> ConnectorResult:
        """Fetch data from the configured source.

        Args:
            source: Source configuration (type depends on connector)
            reference_date: Reference date for temporal sources (default: today)

        Returns:
            ConnectorResult with data and metadata

        Raises:
            ConnectorError: If fetch fails
        """
        pass

    @abstractmethod
    def validate_source(self, source: Any) -> list[str]:
        """Validate source configuration.

        Args:
            source: Source configuration to validate

        Returns:
            List of validation error messages (empty if valid)
        """
        pass

    def get_source_identifier(self, source: Any) -> str:
        """Get a human-readable identifier for the source.

        Override in subclasses for source-specific identifiers.

        Args:
            source: Source configuration

        Returns:
            String identifier for logging/display
        """
        return f"{self.connector_type}://{id(source)}"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(type={self.connector_type!r})"
