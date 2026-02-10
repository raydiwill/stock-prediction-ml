"""HTTP client wrapper for FastAPI backend communication."""

import logging

import httpx
import streamlit as st

from stock_prediction_ml.config.settings import settings

logger = logging.getLogger(__name__)

API_BASE_URL = f"http://{settings.api_host}:{settings.api_port}"


@st.cache_data(ttl=120)
def health_check() -> dict | None:
    """Check if the API backend is healthy.

    Returns:
        dict with keys: status, model_loaded, feast_online_store, model_version
        None if the API is unreachable or returns an error

    Example response:
        {"status": "healthy", "model_loaded": True, ...}
    """
    try:
        response = httpx.get(f"{API_BASE_URL}/health", timeout=5.0)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"Health check failed with status {e.response.status_code}")
        return None
    except httpx.RequestError as e:
        logger.error(f"Health check connection error: {e}")
        return None


@st.cache_data(ttl=300)
def get_model_info() -> dict | None:
    """Fetch champion model info (metrics + diagnostics) from backend.

    Returns:
        dict with keys: model_version, model_alias, run_id, metrics, diagnostics
        None if request fails

    Note:
        Cached for 5 minutes — champion model changes infrequently.
        diagnostics values are base64-encoded PNG strings.
    """
    try:
        response = httpx.get(f"{API_BASE_URL}/model/info", timeout=10.0)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"Model info request failed with status {e.response.status_code}")
        return None
    except httpx.RequestError as e:
        logger.error(f"Model info connection error: {e}")
        return None


def predict(symbol: str, date: str) -> dict | None:
    """Request stock prediction from FastAPI backend.

    Args:
        symbol: Stock ticker (e.g., "AAPL")
        date: Trading date in YYYY-MM-DD format

    Returns:
        dict with prediction results, or None if request failed

    Expected response keys:
        symbol, date, prediction, prediction_label, probability, model_version
    """
    payload = {"symbol": symbol, "date": date}

    try:
        response = httpx.post(
            f"{API_BASE_URL}/predict", 
            json=payload, 
            timeout=10.0  # Predictions may take longer
        )
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as e:
        logger.error(
            f"Prediction failed for {symbol} on {date}: "
            f"status {e.response.status_code}"
        )
        return None
    except httpx.RequestError as e:
        logger.error(f"Prediction connection error for {symbol}: {e}")
        return None
