"""About Model page — champion model metrics and diagnostic plots.

Displays:
    - Model identity (version, alias, run ID)
    - Performance metrics (accuracy, ROC-AUC for test and validation)
    - Diagnostic plots in tabs (feature importance, confusion matrix, ROC curve)
"""

import base64

import streamlit as st

from stock_prediction_ml.ui.components.api_client import get_model_info


def render_metrics_dashboard(metrics: dict) -> None:
    """Display test set performance metrics in a two-column layout.

    Args:
        metrics: Dict containing test performance metrics with keys:
                 - test_accuracy: Classification accuracy (0-1 float)
                 - test_roc_auc: Area under ROC curve (0-1 float)
    """

    col1, col2 = st.columns(2)
    with col1:
        st.metric(label="Accuracy", value=f"{100*metrics['test_accuracy']:.2f}%")
    with col2:
        st.metric(label="ROC AUC", value=f"{100*metrics['test_roc_auc']:.2f}%")


def render_diagnostic_plots(diagnostics: dict) -> None:
    """Display model diagnostic plots in a tabbed interface.

    Creates three tabs showing feature importance, confusion matrix, and ROC curve.
    Each plot is decoded from base64 PNG and displayed with an explanatory caption.

    Args:
        diagnostics: Dict mapping snake_case plot names to base64-encoded PNG strings.
                     Expected keys: feature_importance, confusion_matrix, roc_curve
    """
    plot_list = ["Feature Importance", "Confusion Matrix", "ROC Curve"]
    plot_captions = {
    "Feature Importance": (
        "Shows which features contribute most to model predictions."
    ),
    "Confusion Matrix": (
        "Compares predicted vs actual labels. Diagonal = correct predictions."
    ),
    "ROC Curve": (
        "Trade-off between true positive rate and false positive rate. "
        "Higher AUC = better."
    ),
}


    tabs = st.tabs(plot_list)
    for tab, plot in zip(tabs, plot_list):
        with tab:
            string_list = plot.split()
            formatted_plot_name = "_".join(string_list)
            decoded_image = base64.b64decode(diagnostics[formatted_plot_name.lower()])
            st.image(decoded_image, width="content")
            st.caption(plot_captions[plot])


def main() -> None:
    """Entry point for the About Model page.

    Fetches champion model information from the API and displays:
        1. Performance metrics (accuracy, ROC AUC)
        2. Diagnostic plots (feature importance, confusion matrix, ROC curve)

    If API call fails, shows error message and stops rendering.
    """
    st.header("About the Model")

    model_info = get_model_info()
    if model_info is None:
        st.error("Fetching model info error")
        st.stop()

    st.divider()

    st.subheader("Performance Metrics")
    render_metrics_dashboard(model_info["metrics"])

    st.divider()

    st.subheader("Diagnostic Plots")
    render_diagnostic_plots(model_info["diagnostics"])
