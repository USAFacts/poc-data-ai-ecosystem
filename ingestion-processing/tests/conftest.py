"""Pytest configuration and fixtures."""

import os
from pathlib import Path

import pytest


@pytest.fixture
def manifests_path(tmp_path: Path) -> Path:
    """Create a temporary manifests directory."""
    agencies = tmp_path / "agencies"
    assets = tmp_path / "assets"
    workflows = tmp_path / "workflows"
    agencies.mkdir()
    assets.mkdir()
    workflows.mkdir()
    return tmp_path


@pytest.fixture
def sample_agency_yaml() -> str:
    """Return sample agency YAML content."""
    return """
apiVersion: pipeline/v1
kind: Agency
metadata:
  name: test-agency
  labels:
    category: test
spec:
  fullName: Test Agency
  baseUrl: https://test.gov
  description: A test agency
"""


@pytest.fixture
def sample_asset_yaml() -> str:
    """Return sample asset YAML content."""
    return """
apiVersion: pipeline/v1
kind: Asset
metadata:
  name: test-asset
  labels:
    domain: test
spec:
  agencyRef: test-agency
  description: A test asset
  acquisition:
    type: http
    source:
      url: https://test.gov/data.csv
    format: csv
"""


@pytest.fixture
def sample_workflow_yaml() -> str:
    """Return sample workflow YAML content."""
    return """
apiVersion: pipeline/v1
kind: Workflow
metadata:
  name: test-workflow
spec:
  assetRef: test-asset
  steps:
    - name: acquire
      type: acquisition
      config: {}
"""


@pytest.fixture(autouse=True)
def env_setup(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set up test environment variables."""
    monkeypatch.setenv("MINIO_ENDPOINT", "localhost:9000")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("MINIO_SECRET_KEY", "minioadmin")
    monkeypatch.setenv("MINIO_BUCKET", "test-bucket")
    monkeypatch.setenv("MINIO_SECURE", "false")
