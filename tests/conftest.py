"""Pytest configuration and shared fixtures for UI tests.

This module mocks Streamlit to prevent slow imports and browser/server initialization
during test collection and execution.
"""

import sys
from unittest.mock import MagicMock

import pytest
from loguru import logger

# Mock Streamlit BEFORE any test modules are imported
# This prevents streamlit from initializing its server/browser components
streamlit_mock = MagicMock()
streamlit_mock.cache_data = lambda **kwargs: lambda func: func  # No-op decorator
sys.modules["streamlit"] = streamlit_mock


class LogCaptureFixture:
    """caplog-compatible fixture for capturing loguru logs."""

    def __init__(self) -> None:
        self.records: list[str] = []
        self._handler_id: int | None = None

    def __enter__(self) -> "LogCaptureFixture":
        """Start capturing logs."""
        self.records.clear()

        def capture_sink(message: str) -> None:
            self.records.append(message.rstrip("\n"))

        self._handler_id = logger.add(capture_sink, format="{message}", level=0)
        return self

    def __exit__(self, *args) -> None:
        """Stop capturing logs."""
        if self._handler_id is not None:
            logger.remove(self._handler_id)


@pytest.fixture
def loguru_caplog() -> LogCaptureFixture:
    """Capture loguru logs similar to pytest's caplog.

    Returns:
        LogCaptureFixture: Context manager that captures all loguru logs.

    Usage:
        def test_logging(loguru_caplog):
            with loguru_caplog:
                logger.info("test message")
            assert "test message" in loguru_caplog.records
    """
    return LogCaptureFixture()
