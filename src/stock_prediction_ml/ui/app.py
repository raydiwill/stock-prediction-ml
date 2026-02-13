"""Streamlit application entry point with multi-page navigation."""

import streamlit as st

# from stock_prediction_ml.ui.pages.historical import main as historical_page
from stock_prediction_ml.ui.pages.about_model import main as about_model_page
from stock_prediction_ml.ui.pages.prediction import main as prediction_page

# =============================================================================
# STYLES - Inject custom CSS for hover effects and tool styling
# =============================================================================


def inject_custom_css():
    """Inject custom CSS for sidebar navigation and tool button styling.

    Applies styles for:
        - Hiding default Streamlit multi-page nav
        - Category box containers with bordered grouping
        - Sidebar button hover effects (green glow, background tint)
        - Left-aligned button text within sidebar
    """
    st.markdown(
        """
        <style>
        /* Hide default Streamlit multi-page navigation */
        [data-testid="stSidebarNav"] {
            display: none;
        }

        /* Category box - wraps tools in a bordered container */
        .category-box {
            border: 1px solid #ddd;
            border-radius: 10px;
            padding: 15px;
            margin-bottom: 15px;
            background: rgba(255, 255, 255, 0.05);
        }

        .category-title {
            font-size: 14px;
            font-weight: 600;
            margin-bottom: 10px;
            color: #666;
        }

        /* Style Streamlit buttons in sidebar */
        [data-testid="stSidebar"] button[kind="secondary"] {
            background: transparent;
            border: 2px solid transparent;
            border-radius: 8px;
            padding: 8px 12px;
            transition: all 0.2s ease;
            width: 100%;
        }

        /* Left-align button content */
        [data-testid="stSidebar"] button[kind="secondary"] > div {
            justify-content: flex-start;
        }

        [data-testid="stSidebar"] button[kind="secondary"] [data-testid="stMarkdownContainer"] {
            width: 100%;
        }

        [data-testid="stSidebar"] button[kind="secondary"] [data-testid="stMarkdownContainer"] p {
            text-align: left;
        }

        [data-testid="stSidebar"] button[kind="secondary"]:hover {
            border: 2px solid #4CAF50;
            box-shadow: 0 0 8px rgba(76, 175, 80, 0.3);
            background: rgba(76, 175, 80, 0.1);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


# =============================================================================
# COMPONENTS - Reusable UI pieces
# =============================================================================


def tool_button(emoji: str, label: str, key: str) -> bool:
    """Render a sidebar navigation button with emoji prefix.

    Uses ``st.button`` with full-width stretch. Hover styling is handled
    by CSS injected via ``inject_custom_css()``.

    Args:
        emoji: Icon displayed before the label (e.g., "📈").
        label: Display text for the button (e.g., "Live Prediction").
        key: Unique Streamlit widget key, also used as the page key
            in session state.

    Returns:
        True if the button was clicked this render cycle, False otherwise.
    """
    clicked = st.button(f"{emoji} {label}", key=key, width="stretch")
    return clicked


def category_box(title: str, tools: list[tuple[str, str, str]]):
    """Render a bordered sidebar section containing navigation buttons.

    Each button click updates ``st.session_state.current_page`` to the
    tool's page key, triggering a page switch on the next rerun.

    Args:
        title: Section heading displayed as an uppercase caption.
        tools: List of ``(emoji, label, page_key)`` tuples defining
            the buttons to render inside the container.
    """
    # Use container with border (Streamlit 1.29+)
    with st.container(border=True):
        st.caption(title.upper())
        for emoji, label, key in tools:
            clicked = tool_button(emoji, label, key)
            if clicked:
                st.session_state.current_page = key


# =============================================================================
# PAGES - Content for each section
# =============================================================================


def welcome_page():
    """Render the welcome/landing page shown when no tool is selected.

    Displays a hero section, an "At a Glance" metrics row (model type,
    feature count, tracking tool), and a getting-started guide pointing
    users to the sidebar navigation.
    """

    # Hero section
    st.title("Stock Movement Predictor")
    st.markdown(
        "*ML-powered predictions for daily stock direction using CatBoost and Feast*"
    )

    st.markdown("---")

    # What it does
    st.header("What is this?")
    st.markdown(
        """
        This tool predicts whether a stock will go **up** or **down** the next trading day.

        It uses:
        - Historical price data and technical indicators
        - A CatBoost classifier trained on temporal splits
        - Real-time feature serving via Feast
        """
    )

    # Stats row
    st.header("At a Glance")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label="Model", value="CatBoost", delta="Classifier")
    with col2:
        st.metric(label="Features", value="15+", delta="Technical indicators")
    with col3:
        st.metric(label="Tracking", value="MLflow", delta="Experiment logging")

    st.markdown("---")

    # Quick start
    st.header("Getting Started")
    st.markdown(
        """
        1. **Live Prediction** - Enter a ticker symbol to get tomorrow's prediction
        2. **Historical Data** - Explore past predictions and accuracy
        3. **About Model** - Learn how the model works and its performance metrics

        Use the sidebar to navigate between tools.
        """
    )


# =============================================================================
# NAVIGATION - Define your tool categories and routing
# =============================================================================

# Tool definitions: (emoji, display_name, page_key)
ANALYTICS_TOOLS = [
    ("📈", "Live Prediction", "prediction"),
    ("📊", "Historical Data", "historical"),
]

MODEL_TOOLS = [
    ("🤖", "About Model", "about_model"),
    # Add more as needed: ("📉", "Performance Metrics", "metrics"),
]

# Map page keys to their render functions
PAGE_REGISTRY = {
    "home": welcome_page,
    "prediction": prediction_page,
    # "historical": historical_page,
    "about_model": about_model_page,
}


def render_sidebar():
    """Render the sidebar with branding, tool categories, and footer.

    Layout:
        1. App title and tagline
        2. "Analytics" category (Live Prediction, Historical Data)
        3. "Model" category (About Model)
        4. Footer attribution
    """
    with st.sidebar:
        # Branding
        st.title("📊 Stock Predictor")
        st.caption("ML-powered daily predictions")

        st.markdown("---")

        category_box("Analytics", ANALYTICS_TOOLS)
        category_box("Model", MODEL_TOOLS)

        st.markdown("---")
        st.caption("Built with Streamlit + FastAPI")


def render_current_page():
    """Route to the active page function based on session state.

    Reads ``st.session_state.current_page``, looks up the corresponding
    render function in ``PAGE_REGISTRY``, and calls it. Falls back to
    ``welcome_page`` if the key is missing or unregistered. Catches and
    displays any rendering errors inline.
    """
    current = st.session_state.get("current_page", "home")
    page_func = PAGE_REGISTRY.get(current, welcome_page)

    try:
        page_func()
    except Exception as e:
        print(f"❌ ERROR: {type(e).__name__}: {e}")
        st.error(f"Error loading page: {e}")


# =============================================================================
# MAIN - Entry point
# =============================================================================


def main():
    """Application entry point — configures page, injects CSS, and renders.

    Must be called once per Streamlit run. ``st.set_page_config`` is
    invoked first (Streamlit requirement), followed by session state
    initialization, CSS injection, sidebar, and the active page.
    """

    # Page config (must be first Streamlit command)
    st.set_page_config(
        page_title="Stock Prediction ML",
        page_icon="📊",
        layout="wide",  # "wide" gives more room for content
        initial_sidebar_state="expanded",
    )

    # Initialize session state for navigation
    if "current_page" not in st.session_state:
        st.session_state.current_page = "home"

    # Inject custom CSS
    inject_custom_css()

    # Render app structure
    render_sidebar()
    render_current_page()


if __name__ == "__main__":
    main()
