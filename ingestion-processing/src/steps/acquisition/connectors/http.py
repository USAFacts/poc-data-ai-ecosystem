"""HTTP/HTTPS connector for data acquisition."""

import ssl
from datetime import date

import httpx


def _get_ssl_context() -> ssl.SSLContext:
    """Create an SSL context using the system certificate store.

    Tries truststore (macOS/Windows system certs) first,
    falls back to certifi, then to default.
    """
    try:
        import truststore
        return truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    except ImportError:
        pass
    try:
        import certifi
        ctx = ssl.create_default_context(cafile=certifi.where())
        return ctx
    except ImportError:
        return ssl.create_default_context()

from control.models import HttpSource
from steps.acquisition.connectors.base import Connector, ConnectorError, ConnectorResult
from steps.acquisition.temporal import resolve_temporal_url, resolve_temporal_context


class HttpConnector(Connector):
    """Connector for HTTP/HTTPS data sources.

    Supports:
    - Static URLs
    - Temporal URL templates (fiscal year/quarter patterns)
    - Custom headers
    - Redirect following
    """

    connector_type = "http"

    def resolve_url(
        self, source: HttpSource, reference_date: date | None = None
    ) -> str:
        """Resolve the URL from source configuration.

        Args:
            source: HTTP source configuration
            reference_date: Date for temporal resolution (default: today)

        Returns:
            Resolved URL string

        Raises:
            ConnectorError: If no URL configuration provided
        """
        if source.url:
            return source.url
        elif source.temporal:
            return resolve_temporal_url(source.temporal, reference_date)
        else:
            raise ConnectorError("No URL or temporal config provided")

    def fetch(
        self,
        source: HttpSource,
        reference_date: date | None = None,
    ) -> ConnectorResult:
        """Fetch data from HTTP source.

        Args:
            source: HTTP source configuration
            reference_date: Date for temporal URL resolution (default: today)

        Returns:
            ConnectorResult with data and metadata

        Raises:
            ConnectorError: If fetch fails
        """
        url = self.resolve_url(source, reference_date)

        # Build metadata including temporal info if applicable
        extra_metadata: dict[str, str] = {}
        if source.temporal:
            context = resolve_temporal_context(source.temporal, reference_date)
            extra_metadata = {
                "temporal_pattern": source.temporal.pattern.value,
                "temporal_fiscal_year": str(context.fiscal_year),
                "temporal_quarter": str(context.quarter),
                "temporal_year": str(context.year),
                "temporal_month": str(context.month),
            }

        try:
            ssl_context = _get_ssl_context()
            with httpx.Client(timeout=self.timeout, follow_redirects=True, verify=ssl_context) as client:
                response = client.get(url, headers=source.headers)
                response.raise_for_status()

                metadata = {
                    "source_url": url,
                    "final_url": str(response.url),
                    "content_type": response.headers.get("content-type", ""),
                    "content_length": response.headers.get("content-length", ""),
                    "last_modified": response.headers.get("last-modified", ""),
                    "etag": response.headers.get("etag", ""),
                    "connector_type": self.connector_type,
                    **extra_metadata,
                }

                return ConnectorResult(data=response.content, metadata=metadata)

        except httpx.TimeoutException as e:
            raise ConnectorError(f"Request timed out: {url}") from e
        except httpx.HTTPStatusError as e:
            raise ConnectorError(
                f"HTTP error {e.response.status_code}: {url}"
            ) from e
        except httpx.RequestError as e:
            raise ConnectorError(f"Request failed: {url} - {e}") from e

    def validate_source(self, source: HttpSource) -> list[str]:
        """Validate HTTP source configuration.

        Args:
            source: HTTP source configuration

        Returns:
            List of validation error messages
        """
        errors = []

        if source.url is None and source.temporal is None:
            errors.append("Either 'url' or 'temporal' must be provided")

        if source.url is not None and source.temporal is not None:
            errors.append("Cannot specify both 'url' and 'temporal'")

        if source.temporal:
            if not source.temporal.url_template:
                errors.append("Temporal config requires 'urlTemplate'")

        return errors

    def get_source_identifier(self, source: HttpSource) -> str:
        """Get identifier for the HTTP source."""
        if source.url:
            return source.url
        elif source.temporal:
            return f"temporal:{source.temporal.url_template}"
        return "http://unknown"
