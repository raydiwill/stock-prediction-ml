"""Consolidated utility functions for Streamlit UI."""

from datetime import date, datetime

from stock_prediction_ml.api.utils import next_trading_day as _next_trading_day
from stock_prediction_ml.config.settings import settings


def get_valid_symbols() -> list[str]:
    """Return valid stock ticker symbols from application settings.

    Returns:
        Sorted list of ticker strings (e.g., ``["AAPL", "GOOGL", ...]``).
    """
    valid_tickers = settings.valid_symbols
    return valid_tickers


def get_next_trading_day() -> date:
    """Calculate the next trading day from today, skipping weekends.

    Returns:
        The next weekday date (Mon–Fri). Saturday rolls to Monday,
        Sunday rolls to Monday.

    Note:
        Does not account for market holidays.
    """
    return _next_trading_day(datetime.today().date())


def format_prediction_result(result: dict) -> dict:
    """Transform raw API prediction response into a display-friendly dict.

    Converts the probability float to a percentage string and maps the
    prediction direction to a Streamlit color name and trend emoji.

    Args:
        result: Raw JSON response from ``predict()``, expected keys:
            ``prediction_label`` (str) and ``probability`` (float).

    Returns:
        Dict with keys:
            - ``direction``: "UP" or "DOWN"
            - ``confidence``: Formatted percentage (e.g., "64.70%")
            - ``color``: "green" for UP, "red" for DOWN
            - ``emoji``: Trend emoji matching the direction
    """
    direction = result.get("prediction_label")
    probability = result.get("probability")

    confidence = f"{probability * 100:.2f}"
    color = "green" if direction == "UP" else "red"
    emoji = "📈" if direction == "UP" else "📉"

    return {
        "direction": direction,
        "confidence": f"{confidence}%",
        "color": color,
        "emoji": emoji,
    }
