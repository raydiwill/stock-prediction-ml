"""Streamlit application entry point with multi-page navigation."""

import streamlit as st

from stock_prediction_ml.ui.pages import prediction, historical, about_model


# =============================================================================
# STYLES - Inject custom CSS for hover effects and tool styling
# =============================================================================

def inject_custom_css():
    """Inject custom CSS for tool buttons with hover effects.

    TODO: Define CSS for:
        1. `.tool-button` - Base style for each tool (padding, cursor, border-radius)
        2. `.tool-button:hover` - Hover effect (border glow, background change, scale)
        3. `.category-box` - Container for grouped tools (border, padding, margin)

    Hints:
        - Use `transition` for smooth hover animations
        - `box-shadow` or `border` for the "circle around" effect
        - Consider `transform: scale(1.02)` for subtle grow on hover

    Example structure:
        st.markdown('''
        <style>
        .tool-button {
            padding: ???;
            border-radius: ???;
            cursor: pointer;
            transition: ???;
        }
        .tool-button:hover {
            border: ???;
            box-shadow: ???;
        }
        </style>
        ''', unsafe_allow_html=True)
    """
    st.markdown('''
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
        ''', unsafe_allow_html=True)


# =============================================================================
# COMPONENTS - Reusable UI pieces
# =============================================================================

def tool_button(emoji: str, label: str, key: str) -> bool:
    """Render a clickable tool with emoji and hover effect.

    Args:
        emoji: The emoji to display (e.g., "📈")
        label: The tool name (e.g., "Live Prediction")
        key: Unique key for the button (used for session state)

    Returns:
        True if clicked, False otherwise

    TODO: Two approaches to try:

    Approach A - Simple (st.button with CSS override):
        clicked = st.button(f"{emoji} {label}", key=key, use_container_width=True)
        return clicked

    Approach B - Custom HTML (more control but trickier):
        Use st.markdown with a clickable div, but note that pure HTML clicks
        don't trigger Python. You'd need st.button hidden or use streamlit-extras.

    Hint: Start with Approach A, style via CSS targeting [data-testid="stButton"]
    """
    # YOUR IMPLEMENTATION HERE
    clicked = st.button(f"{emoji} {label}", key=key, width='stretch')
    return clicked


def category_box(title: str, tools: list[tuple[str, str, str]]):
    """Render a category container with multiple tools inside."""
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
    """Render the welcome/landing page content."""

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
    "prediction": prediction,
    "historical": historical,
    "about_model": about_model,
}


def render_sidebar():
    """Render the sidebar with tool navigation.

    TODO:
        1. App branding (logo/title)
        2. Optional: API health indicator
        3. Render tool categories using category_box()
        4. Footer with help text

    Hint: Everything inside `with st.sidebar:` appears in the sidebar
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
    """Render the currently selected page based on session state.

    TODO:
        1. Get current page from st.session_state.current_page
        2. Look up the render function in PAGE_REGISTRY
        3. Call it

    Hint: Use .get() with a default to handle missing keys
    """
    current = st.session_state.get("current_page", "home")
    page_func = PAGE_REGISTRY.get(current, welcome_page)
    page_func()


# =============================================================================
# MAIN - Entry point
# =============================================================================

def main():
    """Main entry point for Streamlit app."""

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
