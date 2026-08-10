"""Unit tests for Stock Prediction API endpoints.

Tests use mocked MLflow and Feast dependencies to run without
external infrastructure. All tests are hermetic and CI-friendly.
"""

from unittest.mock import Mock

import pandas as pd
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from stock_prediction_ml.db.models import Base, PredictionResult, RawStockData
from stock_prediction_ml.db.session import get_db

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
def test_db_session():
    """In-memory SQLite session for hermetic testing."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    TestSession = sessionmaker(bind=engine)
    session = TestSession()
    yield session
    session.close()


@pytest.fixture
def client_with_dependencies(mocker, mock_model, mock_feast_store, test_db_session):
    """Create TestClient with all dependencies loaded (healthy state).

    Patches main.py global variables: MODEL, FEAST_STORE, MODEL_VERSION.
    Overrides get_db to use in-memory test database.
    """
    mocker.patch("stock_prediction_ml.api.main.MODEL", mock_model)
    mocker.patch("stock_prediction_ml.api.main.FEAST_STORE", mock_feast_store)
    mocker.patch("stock_prediction_ml.api.main.MODEL_VERSION", "1")

    from stock_prediction_ml.api.main import app

    def override_get_db():
        try:
            yield test_db_session
        finally:
            pass  # Fixture manages session lifecycle

    app.dependency_overrides[get_db] = override_get_db

    yield TestClient(app, raise_server_exceptions=False)
    app.dependency_overrides.clear()


@pytest.fixture
def client_without_model(mocker, mock_feast_store, test_db_session):
    """Create TestClient with MODEL=None (simulates model load failure)."""
    mocker.patch("stock_prediction_ml.api.main.MODEL", None)
    mocker.patch("stock_prediction_ml.api.main.FEAST_STORE", mock_feast_store)
    mocker.patch("stock_prediction_ml.api.main.MODEL_VERSION", "unknown")

    from stock_prediction_ml.api.main import app

    app.dependency_overrides[get_db] = lambda: test_db_session

    yield TestClient(app, raise_server_exceptions=False)
    app.dependency_overrides.clear()


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

    def test_predict_success_returns_prediction(self, client_with_dependencies, test_db_session):
        """Verify /predict returns 200 with valid prediction when dependencies loaded."""
        # Seed a RawStockData row for the model to attach to
        test_db_session.add(
            RawStockData(
                id=1,
                symbol="AAPL",
                date=pd.to_datetime("2025-12-08"),
                open=150.0,
                close=151.5,
                high=152.0,
                low=149.0,
                volume=1000000,
                adj_close=151.5,
                source="api",
                hash_input="test_hash_001",
                pulled_at=pd.to_datetime("2025-12-08"),
                parquet_path="test_path",
                validated=True,
            )
        )
        test_db_session.commit()

        request_body = {"symbol": "AAPL"}

        response = client_with_dependencies.post("/predict", json=request_body)

        assert response.status_code == 200

        data = response.json()
        required_keys = [
            "prediction",
            "prediction_label",
            "probability",
            "as_of_date",
            "target_date",
        ]
        assert all(key in data for key in required_keys)
        assert data["prediction_label"] in ["UP", "DOWN"]
        assert data["symbol"] == "AAPL"
        assert data["model_version"] == "1"
        assert data["prediction"] in [0, 1]
        assert data["as_of_date"] == "2025-12-08"
        assert data["target_date"] is not None

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
            "/predict", json={"symbol": "AAPL"}
        )

        assert response.status_code == 404

    def test_predict_model_failure_returns_500(self, mocker, mock_feast_store):
        """Verify /predict returns 500 when model.predict() raises an exception."""
        mock_model_error = Mock()
        mock_model_error.predict.side_effect = Exception("Model Failed")

        mocker.patch("stock_prediction_ml.api.main.MODEL", mock_model_error)
        mocker.patch("stock_prediction_ml.api.main.FEAST_STORE", mock_feast_store)
        mocker.patch("stock_prediction_ml.api.main.MODEL_VERSION", "1")

        from stock_prediction_ml.api.main import app

        client = TestClient(app, raise_server_exceptions=False)

        response = client.post(
            "/predict", json={"symbol": "AAPL"}
        )

        assert response.status_code == 500

    def test_predict_dependencies_missing_returns_503(self, client_without_model):
        """Verify /predict returns 503 when MODEL is None (service unavailable)."""
        response = client_without_model.post(
            "/predict", json={"symbol": "AAPL"}
        )

        assert response.status_code == 503


# ==================== PREDICTION PERSISTENCE TESTS ====================


class TestPredictPersistence:
    """Tests for prediction DB persistence in POST /predict."""

    def test_predict_persists_to_newest_raw_data_row(
        self, client_with_dependencies, test_db_session
    ):
        """Verify prediction lands on the newest RawStockData row for the symbol."""
        # Seed two dated rows; prediction should attach to the newest
        test_db_session.add_all([
            RawStockData(
                id=1,
                symbol="AAPL",
                date=pd.to_datetime("2025-12-07"),
                open=150.0,
                close=150.5,
                high=151.0,
                low=149.5,
                volume=1000000,
                adj_close=150.5,
                source="api",
                hash_input="old_hash_001",
                pulled_at=pd.to_datetime("2025-12-07"),
                parquet_path="test_path_old",
                validated=True,
            ),
            RawStockData(
                id=2,
                symbol="AAPL",
                date=pd.to_datetime("2025-12-08"),
                open=150.5,
                close=151.5,
                high=152.0,
                low=150.0,
                volume=1100000,
                adj_close=151.5,
                source="api",
                hash_input="new_hash_001",
                pulled_at=pd.to_datetime("2025-12-08"),
                parquet_path="test_path_new",
                validated=True,
            )
        ])
        test_db_session.commit()

        _ = client_with_dependencies.post(
            "/predict", json={"symbol": "AAPL"}
        )

        queried = (
            test_db_session.query(PredictionResult)
            .filter(PredictionResult.raw_stock_data_id == 2)
            .first()
        )

        assert queried is not None, "Expected PredictionResult row but found None"
        assert queried.raw_stock_data_id == 2, "Prediction should attach to newest row"
        assert queried.prediction in [0, 1]
        assert 0.0 <= queried.probability <= 1.0
        assert queried.raw_data.symbol == "AAPL"

    def test_predict_succeeds_without_raw_data_row(
        self, client_with_dependencies, test_db_session
    ):
        """Verify /predict returns 200 even when no RawStockData row exists (no DB save)."""
        response = client_with_dependencies.post(
            "/predict", json={"symbol": "AAPL"}
        )

        queried = test_db_session.query(PredictionResult).all()

        assert response.status_code == 200
        assert not queried

    def test_predict_persists_when_only_past_rows_exist(
        self, client_with_dependencies, test_db_session
    ):
        """Verify prediction is persisted even when only past RawStockData rows exist."""
        # This was silently failing from the UI before Phase 1
        test_db_session.add(
            RawStockData(
                id=1,
                symbol="AAPL",
                date=pd.to_datetime("2025-12-05"),
                open=149.0,
                close=149.5,
                high=150.0,
                low=148.5,
                volume=900000,
                adj_close=149.5,
                source="api",
                hash_input="past_hash_001",
                pulled_at=pd.to_datetime("2025-12-05"),
                parquet_path="test_path_past",
                validated=True,
            )
        )
        test_db_session.commit()

        response = client_with_dependencies.post(
            "/predict", json={"symbol": "AAPL"}
        )

        queried = test_db_session.query(PredictionResult).all()

        assert response.status_code == 200
        assert len(queried) == 1
        assert queried[0].raw_stock_data_id == 1


# ==================== VALIDATION TESTS ====================


class TestRequestValidation:
    """Tests for Pydantic request validation (schema.py)."""

    def test_invalid_symbol_returns_422(self, client_with_dependencies):
        """Verify /predict returns 422 when symbol not in valid_symbols list."""
        response = client_with_dependencies.post(
            "/predict", json={"symbol": "test"}
        )

        assert response.status_code == 422


# ==================== STOCK HISTORY ENDPOINT TESTS ====================


class TestStockHistory:
    """Tests for GET /stock/history endpoint.

    Key invariant being tested: a prediction made on day D-1 predicts the direction
    for day D (as per build_features.py:93 label). So day D's record should carry
    the prediction from day D-1's row, not its own predictions.
    """

    def test_stock_history_three_days_prediction_alignment(
        self, client_with_dependencies, test_db_session
    ):
        """Core regression test: prediction made on day 1 surfaces on day 2 row.

        Seed: day 1 close=100, day 2 close=110 (UP), day 3 close=105 (DOWN).
        Place a prediction on day 1's row saying UP.
        Assert: day 2's record shows predicted_direction=UP, actual_direction=UP, correct=True.
        Assert: day 1's record shows predicted_direction=None.
        """
        test_db_session.add_all([
            RawStockData(
                id=1,
                symbol="AAPL",
                date=pd.to_datetime("2025-12-08"),
                open=99.5,
                close=100.0,
                high=100.5,
                low=99.0,
                volume=1000000,
                adj_close=100.0,
                source="api",
                hash_input="day1_hash",
                pulled_at=pd.to_datetime("2025-12-08"),
                parquet_path="path1",
                validated=True,
            ),
            RawStockData(
                id=2,
                symbol="AAPL",
                date=pd.to_datetime("2025-12-09"),
                open=100.5,
                close=110.0,
                high=111.0,
                low=99.5,
                volume=1100000,
                adj_close=110.0,
                source="api",
                hash_input="day2_hash",
                pulled_at=pd.to_datetime("2025-12-09"),
                parquet_path="path2",
                validated=True,
            ),
            RawStockData(
                id=3,
                symbol="AAPL",
                date=pd.to_datetime("2025-12-10"),
                open=110.5,
                close=105.0,
                high=110.5,
                low=104.0,
                volume=1050000,
                adj_close=105.0,
                source="api",
                hash_input="day3_hash",
                pulled_at=pd.to_datetime("2025-12-10"),
                parquet_path="path3",
                validated=True,
            ),
        ])
        test_db_session.commit()

        # Add a prediction on day 1's row predicting UP (prediction=1)
        test_db_session.add(
            PredictionResult(
                id=1,
                raw_stock_data_id=1,
                predicted_at=pd.to_datetime("2025-12-08 10:00:00"),
                model_name="stock_prediction_classifier",
                prediction=1,  # UP
                probability=0.68,
                features_used={},
            )
        )
        test_db_session.commit()

        # Query history from day 1 through day 3
        response = client_with_dependencies.get(
            "/stock/history?symbol=AAPL&start_date=2025-12-08&end_date=2025-12-10"
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total_records"] == 3
        records = data["records"]

        # Day 1: no prediction yet (first day has no prev_close seed)
        assert records[0]["date"] == "2025-12-08"
        assert records[0]["close"] == 100.0
        assert records[0]["actual_direction"] is None
        assert records[0]["predicted_direction"] is None
        assert records[0]["correct"] is None

        # Day 2: should have the prediction from day 1
        assert records[1]["date"] == "2025-12-09"
        assert records[1]["close"] == 110.0
        assert records[1]["actual_direction"] == "UP"  # 110 > 100
        assert records[1]["predicted_direction"] == "UP"  # from day 1's prediction
        assert records[1]["probability"] == 0.68
        assert records[1]["correct"] is True  # UP == UP

        # Day 3: no prediction (day 2 had no predictions)
        assert records[2]["date"] == "2025-12-10"
        assert records[2]["close"] == 105.0
        assert records[2]["actual_direction"] == "DOWN"  # 105 < 110
        assert records[2]["predicted_direction"] is None
        assert records[2]["correct"] is None

    def test_stock_history_prediction_before_start_date_surfaces_on_first_record(
        self, client_with_dependencies, test_db_session
    ):
        """Boundary case: prediction on row before start_date must surface on first returned record.

        Seed: day 0 (before range) with prediction, day 1-2 in range.
        Assert: day 1's record carries the prediction from day 0.
        """
        test_db_session.add_all([
            RawStockData(
                id=1,
                symbol="MSFT",
                date=pd.to_datetime("2025-12-07"),
                open=429.5,
                close=430.0,
                high=431.0,
                low=429.0,
                volume=2000000,
                adj_close=430.0,
                source="api",
                hash_input="before_range_hash",
                pulled_at=pd.to_datetime("2025-12-07"),
                parquet_path="path_before",
                validated=True,
            ),
            RawStockData(
                id=2,
                symbol="MSFT",
                date=pd.to_datetime("2025-12-08"),
                open=430.5,
                close=435.0,
                high=436.0,
                low=430.0,
                volume=2100000,
                adj_close=435.0,
                source="api",
                hash_input="day1_in_range_hash",
                pulled_at=pd.to_datetime("2025-12-08"),
                parquet_path="path1",
                validated=True,
            ),
            RawStockData(
                id=3,
                symbol="MSFT",
                date=pd.to_datetime("2025-12-09"),
                open=435.5,
                close=432.0,
                high=435.5,
                low=431.0,
                volume=2050000,
                adj_close=432.0,
                source="api",
                hash_input="day2_in_range_hash",
                pulled_at=pd.to_datetime("2025-12-09"),
                parquet_path="path2",
                validated=True,
            ),
        ])
        test_db_session.commit()

        # Add prediction on day 0's row (before start_date)
        test_db_session.add(
            PredictionResult(
                id=1,
                raw_stock_data_id=1,
                predicted_at=pd.to_datetime("2025-12-07 10:00:00"),
                model_name="stock_prediction_classifier",
                prediction=1,  # UP
                probability=0.72,
                features_used={},
            )
        )
        test_db_session.commit()

        # Query history starting from day 1 (excluding day 0)
        response = client_with_dependencies.get(
            "/stock/history?symbol=MSFT&start_date=2025-12-08&end_date=2025-12-09"
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total_records"] == 2
        records = data["records"]

        # Day 1 (first in range): should carry prediction from day 0 (before range)
        assert records[0]["date"] == "2025-12-08"
        assert records[0]["close"] == 435.0
        assert records[0]["actual_direction"] == "UP"  # 435 > 430
        assert records[0]["predicted_direction"] == "UP"  # from day 0's prediction
        assert records[0]["probability"] == 0.72
        assert records[0]["correct"] is True

        # Day 2: no prediction (day 1 had no predictions)
        assert records[1]["date"] == "2025-12-09"
        assert records[1]["close"] == 432.0
        assert records[1]["actual_direction"] == "DOWN"  # 432 < 435
        assert records[1]["predicted_direction"] is None
        assert records[1]["correct"] is None

    def test_stock_history_empty_range_returns_404(self, client_with_dependencies):
        """Verify /stock/history returns 404 for a date range with no data."""
        response = client_with_dependencies.get(
            "/stock/history?symbol=AAPL&start_date=2025-01-01&end_date=2025-01-02"
        )

        assert response.status_code == 404

    def test_stock_history_invalid_symbol_returns_400(self, client_with_dependencies):
        """Verify /stock/history returns 400 for invalid symbol."""
        response = client_with_dependencies.get(
            "/stock/history?symbol=INVALID&start_date=2025-12-08&end_date=2025-12-10"
        )

        assert response.status_code == 400

    def test_stock_history_end_before_start_returns_400(
        self, client_with_dependencies, test_db_session
    ):
        """Verify /stock/history returns 400 when end_date < start_date."""
        test_db_session.add(
            RawStockData(
                id=1,
                symbol="AAPL",
                date=pd.to_datetime("2025-12-08"),
                open=150.0,
                close=151.0,
                high=152.0,
                low=150.0,
                volume=1000000,
                adj_close=151.0,
                source="api",
                hash_input="test_hash",
                pulled_at=pd.to_datetime("2025-12-08"),
                parquet_path="path",
                validated=True,
            )
        )
        test_db_session.commit()

        response = client_with_dependencies.get(
            "/stock/history?symbol=AAPL&start_date=2025-12-10&end_date=2025-12-08"
        )

        assert response.status_code == 400
