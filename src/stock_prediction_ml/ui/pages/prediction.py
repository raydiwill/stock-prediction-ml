"""Prediction page - Get stock direction predictions."""

import streamlit as st

from stock_prediction_ml.ui.components.api_client import health_check, predict
from stock_prediction_ml.ui.utils import format_prediction_result, get_valid_symbols


def render_prediction_form():
    """Render the stock prediction input form.

    Displays a symbol dropdown followed by a centered "Get Prediction"
    button. The prediction date is determined by the API response, not
    guessed client-side.

    Returns:
        tuple[str, bool]: ``(ticker, was_clicked)``
    """
    st.markdown("#### Select Stock Symbol")
    ticker = st.selectbox(
        "Choose a stock ticker:",
        get_valid_symbols(),
        label_visibility="collapsed",
    )

    st.markdown("")
    _, col_btn, _ = st.columns([1, 1, 1])
    with col_btn:
        click = st.button(
            "🔮 Get Prediction", type="primary", use_container_width=True
        )

    return ticker, click


def render_prediction_result(result: dict, prediction: dict):
    """Display the formatted prediction result with visual styling.

    Renders a two-column section showing the predicted direction
    (colored UP/DOWN with emoji) and model confidence percentage.

    Args:
        result: Formatted dict from ``format_prediction_result()`` with
            keys ``direction``, ``confidence``, ``color``, ``emoji``.
        prediction: Raw API response with keys ``as_of_date``, ``target_date``.
    """
    direction = result.get("direction")
    confidence = result.get("confidence")
    text_color = result.get("color")
    emoji = result.get("emoji")
    target_date = prediction.get("target_date")
    as_of_date = prediction.get("as_of_date")

    st.markdown("---")
    st.markdown("### 📊 Prediction Result")
    st.info(f"📅 Prediction for {target_date}, based on data through {as_of_date}")
    st.markdown("")

    col_pred, col_conf = st.columns(2)
    with col_pred:
        st.markdown("#### Prediction")
        st.markdown(
            f"## :{text_color}[{emoji} {direction}]"
        )
    with col_conf:
        st.markdown("#### Model Confidence")
        st.markdown(f"## {confidence}")


def render_api_status() -> None:
    """Display a health-check status bar above the prediction form.

    Calls ``health_check()`` and renders four metrics in a bordered
    container: API Status, Model Loaded, Feast Store, and Model Version.
    Stops the page with an error if the backend is unreachable.
    """
    result = health_check()

    if result is None:
        st.error("Backend API is unreachable. Please ensure the server is running.")
        st.stop()

    status = result.get("status", "unknown")
    model_loaded = result.get("model_loaded", False)
    feast_ok = result.get("feast_online_store", False)
    model_version = result.get("model_version", "N/A")

    with st.container(border=True):
        col_status, col_model, col_feast, col_version = st.columns(4)
        with col_status:
            st.metric("API Status", status.capitalize())
        with col_model:
            st.metric("Model", "Loaded" if model_loaded else "Not Loaded")
        with col_feast:
            st.metric("Feast Store", "Connected" if feast_ok else "Offline")
        with col_version:
            st.metric("Model Version", model_version)


def main():
    """Entry point for the Live Prediction page.

    Orchestrates the full page flow:
        1. Page header and description
        2. API health-check status bar
        3. Prediction input form (symbol)
        4. On button click — calls ``predict()`` with a loading spinner,
           then displays the result or an error message
    """
    # Page header
    st.title("📈 Live Prediction")
    st.markdown(
        "Get real-time predictions for next-day stock price movements using our ML model"
    )
    st.markdown("")  # Spacing

    render_api_status()

    st.markdown("")  # Spacing

    # Render form and get inputs
    ticker, predict_clicked = render_prediction_form()

    # Handle prediction request
    if predict_clicked:
        # Call API with loading spinner
        with st.spinner("🤖 Analyzing market data..."):
            prediction = predict(ticker)

        # Display result or error
        if not prediction:
            st.error("❌ Unable to generate prediction. Please try again.")
        else:
            result = format_prediction_result(prediction)
            render_prediction_result(result, prediction)
