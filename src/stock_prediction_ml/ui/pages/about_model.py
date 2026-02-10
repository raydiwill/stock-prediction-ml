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
    """Display performance metrics in a 4-column grid.

    Args:
        metrics: Dict with keys like test_accuracy, test_roc_auc,
                 val_accuracy, val_roc_auc (float values).

    Hint:
        - Use st.columns(4) for a row of metrics
        - st.metric(label=..., value=...) for each
        - Consider formatting: f"{value:.1%}" for percentages
        - You could show val metrics as delta below test metrics,
          or keep them as separate st.metric() calls
    """
    pass


def render_diagnostic_plots(diagnostics: dict) -> None:
    """Display diagnostic plots in a tabbed interface.

    Args:
        diagnostics: Dict mapping plot names to base64-encoded PNG strings.
                     Expected keys: feature_importance, confusion_matrix, roc_curve

    Hint:
        - st.tabs(["Feature Importance", "Confusion Matrix", "ROC Curve"])
        - For each tab: decode with base64.b64decode(encoded_str)
        - Display with st.image(decoded_bytes, use_container_width=True)
        - Handle missing plots: check if key exists and value is non-empty
        - Add a brief caption under each plot explaining what it shows
    """
    pass


def main() -> None:
    """Entry point for the About Model page."""
    st.header("About the Model")

    # Fetch model info from API
    # Hint: get_model_info() returns dict or None
    # On None → st.error() + st.stop()

    # Model identity section
    # Hint: show model_version, model_alias, and run_id
    # Ideas: st.markdown() with bold labels, or st.columns() with st.metric()
    # An st.expander("Run Details") for run_id keeps it clean

    # Metrics section
    # render_metrics_dashboard(info["metrics"])

    # Diagnostic plots section
    # render_diagnostic_plots(info["diagnostics"])
    pass
