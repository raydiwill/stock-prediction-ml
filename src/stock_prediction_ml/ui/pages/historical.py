"""Historical Data page — prediction results alongside actual price movements.

Displays a table of daily close prices with actual vs predicted direction,
letting users see where the model got it right (or wrong).
"""

from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st

from stock_prediction_ml.ui.components.api_client import get_historical_data
from stock_prediction_ml.ui.components.plot import build_price_chart
from stock_prediction_ml.ui.utils import get_valid_symbols

# ── Session-state defaults ──────────────────────────────────────────────

_DEFAULTS = {
    "hist_symbol": None,
    "hist_start": datetime.today().date() - timedelta(days=90),
    "hist_end": datetime.today().date(),
}


def _init_session_state() -> None:
    """Seed session-state keys on first run."""
    for key, val in _DEFAULTS.items():
        if key not in st.session_state:
            st.session_state[key] = val


# ── Date presets ────────────────────────────────────────────────────────

_PRESETS: dict[str, int] = {
    "7 days": 7,
    "30 days": 30,
    "90 days": 90,
    "1 year": 365,
}


def render_date_presets() -> None:
    """Render quick-select date range buttons.

    Clicking a preset updates session-state start/end dates.
    """
    st.caption("Quick date range:")
    cols = st.columns(len(_PRESETS))
    for col, (label, days) in zip(cols, _PRESETS.items()):
        with col:
            if st.button(label, width="stretch"):
                st.session_state["hist_start"] = datetime.today().date() - timedelta(
                    days=days
                )
                st.session_state["hist_end"] = datetime.today().date()
                st.rerun()


# ── Filters ─────────────────────────────────────────────────────────────


def render_filters() -> tuple[str, date, date]:
    """Render the filter controls: symbol dropdown, start/end date pickers.

    Returns:
        Tuple of (symbol, start_date, end_date) from widget values.
    """
    symbols = get_valid_symbols()
    default_idx = (
        symbols.index(st.session_state["hist_symbol"])
        if st.session_state["hist_symbol"] in symbols
        else 0
    )

    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("#### Select Stock Symbol")
        ticker = st.selectbox(
            "Choose a stock ticker:",
            symbols,
            index=default_idx,
        )
    with col2:
        st.markdown("#### Historical Start Date")
        start_date = st.date_input(
            "Choose the start date:", st.session_state["hist_start"]
        )
    with col3:
        st.markdown("#### Historical End Date")
        end_date = st.date_input("Choose the end date:", st.session_state["hist_end"])

    # Persist selections
    st.session_state["hist_symbol"] = ticker
    st.session_state["hist_start"] = start_date
    st.session_state["hist_end"] = end_date

    return ticker, start_date, end_date


# ── Metrics ─────────────────────────────────────────────────────────────


def _compute_streak(records: list[dict]) -> str:
    """Compute the current streak of consecutive correct/incorrect predictions.

    Args:
        records: List of DailyRecord dicts sorted by date ascending.

    Returns:
        Human-readable streak string, e.g. "3 correct" or "2 incorrect".
    """
    predicted = [
        r for r in records if r["predicted_direction"] and r["correct"] is not None
    ]
    if not predicted:
        return "N/A"

    # Walk backwards from most recent
    streak_val = predicted[-1]["correct"]
    count = 0
    for r in reversed(predicted):
        if r["correct"] == streak_val:
            count += 1
        else:
            break

    label = "correct" if streak_val else "incorrect"
    return f"{count} {label}"


def _direction_precision(records: list[dict], direction: str) -> str:
    """Compute accuracy for a single predicted direction (UP or DOWN).

    Args:
        records: Full list of DailyRecord dicts.
        direction: "UP" or "DOWN".

    Returns:
        Formatted percentage string or "N/A".
    """
    predicted = [r for r in records if r["predicted_direction"] == direction]
    if not predicted:
        return "N/A"
    correct = [r for r in predicted if r["correct"]]
    return f"{len(correct) / len(predicted):.2%}"


def render_summary_metrics(data: dict) -> None:
    """Display summary stats: 
            total records, 
            predicted count, 
            accuracy, 
            per-direction precision, 
            streak.

    Args:
        data: The HistoricalDataResponse dict from the API.
    """
    records = data["records"]
    predicted_records = [r for r in records if r["predicted_direction"]]
    correct_prediction = [r for r in records if r["correct"]]

    with st.container(border=True):
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        with col1:
            st.metric("Total Records:", data["total_records"])
        with col2:
            st.metric("Predicted:", len(predicted_records))
        with col3:
            accuracy = (
                f"{len(correct_prediction) / len(predicted_records):.2%}"
                if predicted_records
                else "N/A"
            )
            st.metric("Accuracy:", accuracy)
        with col4:
            st.metric("UP Precision:", _direction_precision(records, "UP"))
        with col5:
            st.metric("DOWN Precision:", _direction_precision(records, "DOWN"))
        with col6:
            st.metric("Streak:", _compute_streak(records))


# ── Results table ───────────────────────────────────────────────────────


def _color_row(row: pd.Series) -> list[str]:
    """Apply green/red background based on prediction correctness.

    Args:
        row: A single DataFrame row.

    Returns:
        List of CSS styles, one per cell.
    """
    if row.get("Correct") is True:
        return ["background-color: rgba(0, 204, 150, 0.15)"] * len(row)
    if row.get("Correct") is False:
        return ["background-color: rgba(239, 85, 59, 0.15)"] * len(row)
    return [""] * len(row)


def render_results_table(records: list[dict], predicted_only: bool = False) -> None:
    """Render the main data table with color-coded rows.

    Args:
        records: List of DailyRecord dicts from the API.
        predicted_only: If True, show only rows that have a prediction.
    """
    df = pd.DataFrame(records)
    rename_dict = {
        "date": "Date",
        "close": "Close",
        "actual_direction": "Actual",
        "predicted_direction": "Predicted",
        "probability": "Probability",
        "correct": "Correct",
    }

    df = df.rename(columns=rename_dict)
    df = df.sort_values(by=["Date"], ascending=False)

    if predicted_only:
        df = df[df["Predicted"].notna()]

    styled = df.style.apply(_color_row, axis=1)

    st.dataframe(
        styled,
        width="stretch",
        hide_index=True,
        column_config={
            "Close": st.column_config.NumberColumn(format="$%.2f"),
            "Probability": st.column_config.NumberColumn(format="%.1f%%"),
            "Correct": st.column_config.CheckboxColumn(default=False),
        },
    )


# ── Page entry point ────────────────────────────────────────────────────


def main() -> None:
    """Entry point for the Historical Data page.

    Orchestrates: header → presets → filters → load button → chart + summary + table.
    """
    _init_session_state()

    st.header("Historical Data")
    st.markdown("*View prediction results alongside actual price movements*")

    render_date_presets()
    symbol, start_date, end_date = render_filters()

    if st.button("Fetch data", type="primary"):
        with st.spinner("Fetching historical data ..."):
            records = get_historical_data(
                symbol=symbol, start_date=start_date, end_date=end_date
            )

            if records is None:
                st.error("Failed to fetch historical data!")
                st.stop()

            if records["total_records"] == 0:
                st.warning("No data found for this date range!")
                st.stop()

            st.session_state["hist_data"] = records

    # Render results from session state so they survive widget interactions
    records = st.session_state.get("hist_data")
    if records:
        chart_df = pd.DataFrame(records["records"]).rename(
            columns={
                "date": "Date",
                "close": "Close",
                "predicted_direction": "Predicted",
                "correct": "Correct",
            }
        )
        st.plotly_chart(build_price_chart(chart_df), width="stretch")

        render_summary_metrics(records)

        predicted_only = st.checkbox("Show only predicted rows")
        render_results_table(records["records"], predicted_only=predicted_only)
