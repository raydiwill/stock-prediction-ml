"""Plotly chart builders for the Streamlit dashboard.

Dependencies:
    plotly (install via ``pip install plotly``)
"""

import pandas as pd
import plotly.graph_objects as go


def build_price_chart(df: pd.DataFrame) -> go.Figure:
    """Build a line chart of close prices with colored prediction markers.

    Args:
        df: DataFrame with columns: Date, Close, Predicted, Correct.

    Returns:
        Plotly Figure with price line and prediction overlay dots.
    """
    fig = go.Figure()

    # Base price line
    fig.add_trace(
        go.Scatter(
            x=df["Date"],
            y=df["Close"],
            mode="lines",
            name="Close Price",
            line={"color": "#636EFA", "width": 2},
        )
    )

    # Correct predictions — green dots
    correct = df[df["Correct"] == True]  # noqa: E712
    if not correct.empty:
        fig.add_trace(
            go.Scatter(
                x=correct["Date"],
                y=correct["Close"],
                mode="markers",
                name="Correct",
                marker={"color": "#00CC96", "size": 10, "symbol": "circle"},
            )
        )

    # Incorrect predictions — red dots
    incorrect = df[(df["Predicted"].notna()) & (df["Correct"] == False)]  # noqa: E712
    if not incorrect.empty:
        fig.add_trace(
            go.Scatter(
                x=incorrect["Date"],
                y=incorrect["Close"],
                mode="markers",
                name="Incorrect",
                marker={"color": "#EF553B", "size": 10, "symbol": "x"},
            )
        )

    fig.update_layout(
        title="Close Price with Prediction Overlay",
        xaxis_title="Date",
        yaxis_title="Price ($)",
        template="plotly_dark",
        height=400,
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02},
    )

    return fig
