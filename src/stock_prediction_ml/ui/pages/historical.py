"""Historical Data page — prediction results alongside actual price movements.

Displays a table of daily close prices with actual vs predicted direction,
letting users see where the model got it right (or wrong).
"""

from datetime import date, timedelta, datetime

import streamlit as st
import pandas as pd

from stock_prediction_ml.ui.utils import get_valid_symbols
from stock_prediction_ml.ui.components.api_client import get_historical_data


def render_filters() -> tuple[str, date, date]:
    """Render the filter controls: symbol dropdown, start/end date pickers.

    Returns:
        Tuple of (symbol, start_date, end_date) from widget values.
    """
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("#### Select Stock Symbol")
        ticker = st.selectbox(
            "Choose a stock ticker:",
            get_valid_symbols(),
            label_visibility="collapsed",
        )
    with col2:
        st.markdown("#### Historical Start Date")
        start_date = st.date_input(
            "Choose the start date:", datetime.today() - timedelta(days=90)
        )
    with col3:
        st.markdown("#### Historical End Date")
        end_date = st.date_input("Choose the end date:", datetime.today())

    return ticker, start_date, end_date


def render_summary_metrics(data: dict) -> None:
    """Display summary stats: total records, how many had predictions, accuracy.

    Args:
        data: The HistoricalDataResponse dict from the API.
    """
    predicted_records = [
        record for record in data["records"] if record["predicted_direction"]
    ]
    correct_predition = [
        record for record in data["records"] if record["correct"]
    ]
    with st.container(border=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Records fetch:", data["total_records"])
        with col2:
            
            st.metric("How many were predicted:", len(predicted_records))
        with col3:
            st.metric("Accuracy:", len(correct_predition) / len(predicted_records))


def render_results_table(records: list[dict]) -> None:
    """Render the main data table: date, close, actual, predicted, correct.

    Args:
        records: List of DailyRecord dicts from the API.
    """
    df = pd.DataFrame(records)
    rename_dict = {
        "date": "Date",
        "close": "Close",
        "actual_direction": "Actual",
        "predicted_direction": "Predicted",
        "correct": "Correct",
    }

    df = df.rename(columns=rename_dict)
    df = df.sort_values(by=["Date"], ascending=False)

    st.dataframe(
        df,
        width="stretch",
        hide_index=True,
        column_config={
            "Close": st.column_config.NumberColumn(format="$%.2f"),
            "Correct": st.column_config.CheckboxColumn(default=False),
        },
    )


def main() -> None:
    """Entry point for the Historical Data page.

    Orchestrates: header → filters → load button → summary + table.
    """
    st.header("Historical Data")
    st.markdown("*View prediction results alongside actual price movements*")

    symbol, start_date, end_date = render_filters()

    button = st.button("Fetch data", type="primary")
    if button:
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

            render_summary_metrics(records)
            render_results_table(records["records"])
