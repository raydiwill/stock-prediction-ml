"""Unit tests for Stock Prediction API endpoints.

Tests use mocked MLflow and Feast dependencies to run without
external infrastructure. All tests are hermetic and CI-friendly.
"""

from unittest.mock import Mock

import pandas as pd
import pytest
from fastapi.testclient import TestClient

# ==================== FIXTURES ====================


@pytest.fixture
def mock_features_df():
    """Create fake feature DataFrame matching Feast online store output.

    Columns come from stock_prediction_service in feature_services.py:
    - stock_basic_features: open, high, low, close, adj_close, volume
    - stock_technical_features: returns, RSI, MACD, SMA, etc.
    - stock_timeseries_features: day_of_week, month, day_of_month, quarter, is_quarter_end

    Note: Feast adds 'symbol' column automatically.
    """
    return pd.DataFrame(
        {
            # Entity + timestamp (Feast includes these)
            "symbol": ["AAPL"],
            # stock_basic_features (Float32)
            "open": [150.0],
            "high": [152.0],
            "low": [149.0],
            "close": [151.5],
            "adj_close": [151.5],
            "volume": [1000000.0],
            # stock_technical_features (Float32)
            "high_low": [3.0],
            "close_open": [1.5],
            "return": [0.01],
            "return_lag_1": [0.005],
            "return_lag_2": [0.002],
            "return_lag_5": [0.015],
            "return_lag_10": [0.025],
            "return_roll_mean_5": [0.008],
            "return_roll_std_5": [0.005],
            "return_roll_mean_10": [0.012],
            "return_roll_std_10": [0.007],
            "sma_10": [150.5],
            "sma_20": [150.0],
            "rsi_14": [55.0],
            "ema_12": [150.8],
            "ema_26": [150.2],
            "macd": [0.6],
            "macd_signal": [0.5],
            # stock_timeseries_features (Int32)
            "day_of_week": [2],  # Wednesday
            "month": [2],  # February
            "day_of_month": [4],
            "quarter": [1],
            "is_quarter_end": [0],
        }
    )


@pytest.fixture
def mock_model_prediction():
    """Create fake model prediction output matching MLflow pyfunc format.

    Returns DataFrame with columns: prediction_class, prediction_proba_up, prediction_proba_down.
    """
    return pd.DataFrame(
        {
            "prediction_class": [1],
            "prediction_proba_up": [0.68],
            "prediction_proba_down": [0.32],
        }
    )


@pytest.fixture
def mock_model(mock_model_prediction):
    """Mock the MLflow pyfunc model."""
    mock = Mock()
    mock.predict.return_value = mock_model_prediction
    return mock


@pytest.fixture
def mock_feast_store(mock_features_df):
    """Mock the Feast FeatureStore with chained method calls.

    Mocks: store.get_online_features().to_df() -> DataFrame
    """
    mock = Mock()
    mock.get_online_features.return_value.to_df.return_value = mock_features_df
    mock.get_feature_service.return_value = "mocked_service"
    return mock


@pytest.fixture
def client_with_dependencies(mocker, mock_model, mock_feast_store):
    """Create TestClient with all dependencies loaded (healthy state).

    Patches main.py global variables: MODEL, FEAST_STORE, MODEL_VERSION.
    """
    mocker.patch("stock_prediction_ml.api.main.MODEL", mock_model)
    mocker.patch("stock_prediction_ml.api.main.FEAST_STORE", mock_feast_store)
    mocker.patch("stock_prediction_ml.api.main.MODEL_VERSION", "1")

    from stock_prediction_ml.api.main import app

    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def client_without_model(mocker, mock_feast_store):
    """Create TestClient with MODEL=None (simulates model load failure)."""
    mocker.patch("stock_prediction_ml.api.main.MODEL", None)
    mocker.patch("stock_prediction_ml.api.main.FEAST_STORE", mock_feast_store)
    mocker.patch("stock_prediction_ml.api.main.MODEL_VERSION", "unknown")

    from stock_prediction_ml.api.main import app

    return TestClient(app, raise_server_exceptions=False)


# ==================== HEALTH ENDPOINT TESTS ====================


class TestHealthEndpoint:
    """Tests for GET /health endpoint."""

    def test_health_returns_healthy_when_all_dependencies_loaded(
        self, client_with_dependencies
    ):
        """Verify /health returns 200 with status=healthy when all dependencies are loaded."""
        response = client_with_dependencies.get("/health")

        assert response.status_code == 200

        data = response.json()
        assert data["status"] == "healthy"
        assert data["model_loaded"]
        assert data["feast_online_store"]

    def test_health_returns_unhealthy_when_model_missing(self, client_without_model):
        """Verify /health returns status=unhealthy when MODEL is None."""
        response = client_without_model.get("/health")

        data = response.json()
        assert data["status"] == "unhealthy"
        assert not data["model_loaded"]


# ==================== PREDICT ENDPOINT TESTS ====================


class TestPredictEndpoint:
    """Tests for POST /predict endpoint."""

    def test_predict_success_returns_prediction(self, client_with_dependencies):
        """Verify /predict returns 200 with valid prediction when dependencies loaded."""
        request_body = {
            "symbol": "AAPL",
            "date": "2025-12-08",
        }

        response = client_with_dependencies.post("/predict", json=request_body)

        assert response.status_code == 200

        data = response.json()
        assert all(
            key in data for key in ["prediction", "prediction_label", "probability"]
        )
        assert data["prediction_label"] in ["UP", "DOWN"]
        assert data["symbol"] == "AAPL"
        assert data["model_version"] == "1"
        assert data["prediction"] in [0, 1]

    def test_predict_missing_features_returns_404(self, mocker, mock_model):
        """Verify /predict returns 404 when Feast returns features with all NaN values."""
        mock_feast_empty = Mock()
        empty_df = pd.DataFrame(
            {
                "symbol": ["AAPL"],
                "close": [None],
            }
        )
        mock_feast_empty.get_online_features.return_value.to_df.return_value = empty_df
        mock_feast_empty.get_feature_service.return_value = "mocked"

        mocker.patch("stock_prediction_ml.api.main.MODEL", mock_model)
        mocker.patch("stock_prediction_ml.api.main.FEAST_STORE", mock_feast_empty)
        mocker.patch("stock_prediction_ml.api.main.MODEL_VERSION", "1")

        from stock_prediction_ml.api.main import app

        client = TestClient(app, raise_server_exceptions=False)

        response = client.post(
            "/predict", json={"symbol": "AAPL", "date": "2025-12-08"}
        )

        assert response.status_code == 404

    def test_predict_model_failure_returns_500(
        self, mocker, mock_feast_store, mock_features_df
    ):
        """Verify /predict returns 500 when model.predict() raises an exception."""
        mock_model_error = Mock()
        mock_model_error.predict.side_effect = Exception("Model Failed")

        mocker.patch("stock_prediction_ml.api.main.MODEL", mock_model_error)
        mocker.patch("stock_prediction_ml.api.main.FEAST_STORE", mock_feast_store)
        mocker.patch("stock_prediction_ml.api.main.MODEL_VERSION", "1")

        from stock_prediction_ml.api.main import app

        client = TestClient(app, raise_server_exceptions=False)

        response = client.post(
            "/predict", json={"symbol": "AAPL", "date": "2025-12-08"}
        )

        assert response.status_code == 500

    def test_predict_dependencies_missing_returns_503(self, client_without_model):
        """Verify /predict returns 503 when MODEL is None (service unavailable)."""
        response = client_without_model.post(
            "/predict", json={"symbol": "AAPL", "date": "2026-02-04"}
        )

        assert response.status_code == 503


# ==================== VALIDATION TESTS ====================


class TestRequestValidation:
    """Tests for Pydantic request validation (schema.py)."""

    def test_invalid_symbol_returns_422(self, client_with_dependencies):
        """Verify /predict returns 422 when symbol not in valid_symbols list."""
        response = client_with_dependencies.post(
            "/predict", json={"symbol": "test", "date": "2026-02-04"}
        )

        assert response.status_code == 422

    def test_weekend_date_returns_422(self, client_with_dependencies):
        """Verify /predict returns 422 when date is a weekend (markets closed)."""
        response = client_with_dependencies.post(
            "/predict", json={"symbol": "AAPL", "date": "2025-12-07"}
        )

        assert response.status_code == 422
