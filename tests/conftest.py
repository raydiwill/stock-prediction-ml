"""Pytest configuration and shared fixtures for UI tests.

This module mocks Streamlit to prevent slow imports and browser/server initialization
during test collection and execution.
"""

import sys
from unittest.mock import MagicMock

# Mock Streamlit BEFORE any test modules are imported
# This prevents streamlit from initializing its server/browser components
streamlit_mock = MagicMock()
streamlit_mock.cache_data = lambda **kwargs: lambda func: func  # No-op decorator
sys.modules["streamlit"] = streamlit_mock
