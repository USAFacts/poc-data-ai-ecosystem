"""Tests for base step class."""

from datetime import datetime

import pytest

from pipeline.steps.base import Step, StepResult, StepStatus


class TestStepResult:
    """Tests for StepResult class."""

    def test_duration_calculation(self) -> None:
        """Test duration calculation."""
        start = datetime(2024, 1, 1, 12, 0, 0)
        end = datetime(2024, 1, 1, 12, 0, 10)

        result = StepResult(
            status=StepStatus.SUCCESS,
            started_at=start,
            completed_at=end,
        )

        assert result.duration_seconds == 10.0

    def test_duration_none_when_not_completed(self) -> None:
        """Test duration is None when not completed."""
        result = StepResult(
            status=StepStatus.RUNNING,
            started_at=datetime.utcnow(),
        )

        assert result.duration_seconds is None

    def test_to_dict(self) -> None:
        """Test serialization to dict."""
        start = datetime(2024, 1, 1, 12, 0, 0)
        end = datetime(2024, 1, 1, 12, 0, 10)

        result = StepResult(
            status=StepStatus.SUCCESS,
            started_at=start,
            completed_at=end,
            output={"key": "value"},
        )

        data = result.to_dict()

        assert data["status"] == "success"
        assert data["output"] == {"key": "value"}
        assert data["duration_seconds"] == 10.0
