"""REST API connector for data acquisition."""

import os
from datetime import date

import httpx

from control.models import ApiSource, AuthType
from steps.acquisition.connectors.base import Connector, ConnectorError, ConnectorResult


class ApiConnector(Connector):
    """Connector for REST API data sources.

    Supports:
    - Multiple HTTP methods (GET, POST, etc.)
    - Query parameters
    - Custom headers
    - Authentication (API key, Bearer token, Basic auth)
    """

    connector_type = "api"

    def _build_headers(self, source: ApiSource) -> dict[str, str]:
        """Build request headers including authentication.

        Args:
            source: API source configuration

        Returns:
            Headers dictionary

        Raises:
            ConnectorError: If required auth credentials not available
        """
        headers = dict(source.headers)

        if source.auth:
            if source.auth.type == AuthType.API_KEY:
                if source.auth.key_env_var:
                    api_key = os.environ.get(source.auth.key_env_var, "")
                    if not api_key:
                        raise ConnectorError(
                            f"Environment variable {source.auth.key_env_var} not set"
                        )
                    headers[source.auth.header_name] = api_key

            elif source.auth.type == AuthType.BEARER:
                if source.auth.key_env_var:
                    token = os.environ.get(source.auth.key_env_var, "")
                    if not token:
                        raise ConnectorError(
                            f"Environment variable {source.auth.key_env_var} not set"
                        )
                    headers["Authorization"] = f"Bearer {token}"

        return headers

    def _get_basic_auth(self, source: ApiSource) -> tuple[str, str] | None:
        """Get basic auth credentials if configured.

        Args:
            source: API source configuration

        Returns:
            Tuple of (username, password) or None
        """
        if source.auth and source.auth.type == AuthType.BASIC:
            if source.auth.key_env_var:
                creds = os.environ.get(source.auth.key_env_var, "")
                if ":" in creds:
                    username, password = creds.split(":", 1)
                    return (username, password)
        return None

    def build_url(self, source: ApiSource) -> str:
        """Build full URL from source configuration.

        Args:
            source: API source configuration

        Returns:
            Full URL string
        """
        base = source.base_url.rstrip("/")
        endpoint = source.endpoint.lstrip("/")
        return f"{base}/{endpoint}"

    def fetch(
        self,
        source: ApiSource,
        reference_date: date | None = None,
    ) -> ConnectorResult:
        """Fetch data from API source.

        Args:
            source: API source configuration
            reference_date: Unused, for interface compatibility

        Returns:
            ConnectorResult with data and metadata

        Raises:
            ConnectorError: If fetch fails
        """
        url = self.build_url(source)
        headers = self._build_headers(source)
        auth = self._get_basic_auth(source)

        try:
            with httpx.Client(timeout=self.timeout, follow_redirects=True) as client:
                response = client.request(
                    method=source.method,
                    url=url,
                    params=source.params if source.params else None,
                    headers=headers,
                    auth=auth,
                )
                response.raise_for_status()

                metadata = {
                    "source_url": url,
                    "final_url": str(response.url),
                    "content_type": response.headers.get("content-type", ""),
                    "content_length": response.headers.get("content-length", ""),
                    "api_base_url": source.base_url,
                    "api_endpoint": source.endpoint,
                    "api_method": source.method,
                    "connector_type": self.connector_type,
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

    def validate_source(self, source: ApiSource) -> list[str]:
        """Validate API source configuration.

        Args:
            source: API source configuration

        Returns:
            List of validation error messages
        """
        errors = []

        if not source.base_url:
            errors.append("API source requires 'base_url'")

        if not source.endpoint:
            errors.append("API source requires 'endpoint'")

        if source.auth:
            if source.auth.key_env_var:
                env_value = os.environ.get(source.auth.key_env_var)
                if not env_value:
                    errors.append(
                        f"Auth environment variable '{source.auth.key_env_var}' not set"
                    )

        return errors

    def get_source_identifier(self, source: ApiSource) -> str:
        """Get identifier for the API source."""
        return f"{source.base_url}{source.endpoint}"
