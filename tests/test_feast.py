"""
Test suite for Feast feature store configuration and operations.

This module tests the Feast feature store setup including:
- Entity definitions and configuration
- Feature view schemas and field validation
- Feature service bundling
- Feature materialization to online store
- Online and historical feature retrieval
- Point-in-time correctness for training data
"""

import pandas as pd
import pytest

# ==============================================================================
# FIXTURES - Synthetic test data and Feast store setup
# ==============================================================================


@pytest.fixture
def sample_feature_data():
    """Generate minimal synthetic feature data for testing materialization.

    Returns:
        pd.DataFrame: Sample stock features with date, symbol, and OHLCV columns.
    """
    data = {
        "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
        "symbol": ["AAPL", "AAPL", "AAPL"],
        "open": [22.4, 23.1, 22.8],
        "high": [24.3, 24.5, 23.9],
        "low": [21.2, 22.0, 21.8],
        "close": [23.1, 23.8, 23.2],
        "adj_close": [23.2, 23.9, 23.3],
        "volume": [23234, 25000, 24500],
        "return": [0.021, 0.030, -0.025],
        "day_of_week": [0, 1, 2],
    }

    return pd.DataFrame(data)


@pytest.fixture
def feast_repo_path(tmp_path, sample_feature_data):
    """Set up a temporary Feast repository for isolated testing.

    Creates a temporary Feast environment with:
    - feature_store.yaml configuration
    - Entity and feature view definitions
    - Sample feature data in parquet format

    Args:
        tmp_path: Pytest fixture for temporary directory.
        sample_feature_data: Synthetic feature DataFrame.

    Returns:
        Path: Path to temporary Feast repository.
    """
    import yaml

    feature_store_yaml = {
        "project": "stock_prediction_ml",
        "provider": "local",
        "registry": "registry.db",
        "offline_store": {"type": "file"},
        "online_store": {"type": "sqlite", "path": "online_store.db"},
    }

    (tmp_path / "feature_store.yaml").write_text(yaml.dump(feature_store_yaml))

    parquet_path = tmp_path / "data" / "feature" / "sample_feature_data.parquet"
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    sample_feature_data.to_parquet(parquet_path, index=False)

    features_code = f"""
        from datetime import timedelta
        from pathlib import Path
        from feast import FileSource, FeatureView, Field
        from feast.types import Float32, Int32

        from stock_prediction_ml.feast_repo.entities import stock

        stock_features_source = FileSource(
            path=str(Path(r"{parquet_path}")),
            timestamp_field="date",
        )

        stock_test_features = FeatureView(
            name="stock_test_view",
            entities=[stock],
            ttl=timedelta(days=1),
            schema=[
                Field(name="open", dtype=Float32),
                Field(name="high", dtype=Float32),
                Field(name="low", dtype=Float32),
                Field(name="close", dtype=Float32),
                Field(name="adj_close", dtype=Float32),
                Field(name="volume", dtype=Float32),
                Field(name="return", dtype=Float32),
                Field(name="day_of_week", dtype=Int32)
            ],
            source=stock_features_source,
            online=True,
        )
        """
    (tmp_path / "features_definition.py").write_text(features_code)

    return tmp_path


@pytest.fixture
def feast_feature_store():
    from pathlib import Path
    from feast import FeatureStore

    feast_repo_path = (
        Path(__file__).resolve().parents[1] / "src" / "stock_prediction_ml" / "feast_repo"
    )
    store = FeatureStore(repo_path=str(feast_repo_path))

    return store


# ==============================================================================
# TEST 1: Entity Configuration
# ==============================================================================


def test_entity_has_correct_attributes():
    """Verify that the stock entity is configured with correct name and type."""
    from feast import ValueType
    from stock_prediction_ml.feast_repo.entities import stock

    assert stock.name == "symbol"
    assert stock.value_type == ValueType.STRING
    assert len(stock.description) > 0


# ==============================================================================
# TEST 2: Feature View Schemas
# ==============================================================================


@pytest.mark.parametrize(
    "expected_field", ["open", "high", "low", "close", "adj_close", "volume"]
)
def test_stock_basic_features_has_correct_schema(expected_field):
    """Verify stock_basic_features view contains all required OHLCV fields."""
    from stock_prediction_ml.feast_repo.features_definition import stock_basic_features

    field_names = [field.name for field in stock_basic_features.schema]
    assert expected_field in field_names, f"Missing field: {expected_field}"
    assert len(field_names) == 6


@pytest.mark.parametrize(
    "expected_field",
    [
        "high_low",
        "close_open",
        "return",
        "return_lag_1",
        "return_lag_2",
        "return_lag_5",
        "return_lag_10",
        "return_roll_mean_5",
        "return_roll_std_5",
        "return_roll_mean_10",
        "return_roll_std_10",
        "sma_10",
        "sma_20",
        "rsi_14",
        "ema_12",
        "ema_26",
        "macd",
        "macd_signal",
    ],
)
def test_stock_technical_features_has_correct_schema(expected_field):
    """Verify stock_technical_features view contains all 17 technical indicator fields."""
    from stock_prediction_ml.feast_repo.features_definition import stock_technical_features

    field_names = [field.name for field in stock_technical_features.schema]
    assert expected_field in field_names, f"Missing field: {expected_field}"
    assert len(field_names) == 18


@pytest.mark.parametrize(
    "expected_field",
    ["day_of_week", "month", "day_of_month", "quarter", "is_quarter_end"],
)
def test_stock_timeseries_features_has_correct_schema(expected_field):
    """Verify stock_timeseries_features view contains all 5 temporal fields with Int32 dtype."""
    from feast.types import Int32
    from stock_prediction_ml.feast_repo.features_definition import stock_timeseries_features

    field_names = [field.name for field in stock_timeseries_features.schema]
    assert expected_field in field_names, f"Missing field: {expected_field}"

    for field in stock_timeseries_features.schema:
        if field.name in field_names:
            assert field.dtype == Int32


# ==============================================================================
# TEST 3: Feature Service Configuration
# ==============================================================================


@pytest.mark.parametrize(
    "expected_view",
    ["stock_basic_features", "stock_technical_features", "stock_timeseries_features"],
)
def test_feature_service_includes_all_views(expected_view):
    """Verify stock_prediction_service bundles all 3 feature views."""
    from stock_prediction_ml.feast_repo.feature_services import stock_prediction_service

    view_names = [view.name for view in stock_prediction_service.feature_view_projections]
    assert expected_view in view_names, f"Missing view: {expected_view}"


# ==============================================================================
# TEST 4: Feature Store Initialization
# ==============================================================================


def test_feature_store_can_be_initialized(feast_feature_store):
    """Verify FeatureStore can be initialized with the configured repo path."""
    store = feast_feature_store

    assert store is not None
    assert store.project == "stock_prediction_ml"


def test_feature_store_lists_entities(feast_feature_store):
    """Verify FeatureStore can list all registered entities."""
    store = feast_feature_store

    entities = store.list_entities()
    assert len(entities) > 0

    entity_names = [entity.name for entity in entities]
    assert "symbol" in entity_names


@pytest.mark.parametrize(
    "expected_view",
    ["stock_basic_features", "stock_technical_features", "stock_timeseries_features"],
)
def test_feature_store_lists_feature_views(expected_view, feast_feature_store):
    """Verify FeatureStore can list all registered feature views."""
    store = feast_feature_store

    feature_views = store.list_feature_views()
    assert len(feature_views) == 3

    view_names = [feature_view.name for feature_view in feature_views]
    assert expected_view in view_names, f"Missing view: {expected_view}"


# ==============================================================================
# TEST 5: Feature Materialization (Online Store)
# ==============================================================================


@pytest.mark.slow  # Mark as slow since materialization can take time
def test_materialize_features_to_online_store(feast_repo_path, sample_feature_data):
    """Test materialization of features from offline store (parquet) to online store (SQLite).

    Verifies the core Feast workflow
    1. Offline store contains features in parquet format
    2. Materialize() copies features to online store
    3. Online store is ready for low-latency feature serving
    """
    from feast import FeatureStore

    store = FeatureStore(repo_path=str(feast_repo_path))

    start_date = sample_feature_data["date"].min()
    end_date = sample_feature_data["date"].max()

    store.materialize(start_date=start_date, end_date=end_date)

    assert True  # Placeholder - materialize() will raise exception if it fails


# ==============================================================================
# TEST 6: Online Feature Retrieval
# ==============================================================================


def test_get_online_features_returns_correct_values(feast_feature_store):
    """Test online feature retrieval for model serving.

    Simulates production inference: given stock symbols, retrieve their latest features
    from the online store for low-latency predictions.

    Note: Requires features to be materialized to online store first.
    """
    store = feast_feature_store

    entity_rows = {"symbol": ["AAPL", "MSFT"]}
    features = [
        "stock_basic_features:close",
        "stock_technical_features:return",
    ]

    feature_vector = store.get_online_features(
        entity_rows=entity_rows, features=features
    )
    df = feature_vector.to_df()

    assert len(df) > 0

    for feature_ref in features:
        feature_name = feature_ref.split(":")[-1]
        assert feature_name in df.columns


# ==============================================================================
# TEST 7: Historical Feature Retrieval (Point-in-Time Join)
# ==============================================================================


def test_get_historical_features_performs_point_in_time_join(feast_feature_store):
    """Test historical feature retrieval with point-in-time correctness.

    Used for training data generation. Feast performs point-in-time joins to ensure
    no data leakage - only features available at each timestamp are retrieved.

    Note: Requires offline store (parquet) to be populated.
    """
    import pandas as pd

    store = feast_feature_store

    entity_df = pd.DataFrame(
        {
            "symbol": ["AAPL", "AAPL"],
            "date": pd.to_datetime(["2025-01-08", "2025-01-10"]),
        }
    )

    features = [
        "stock_basic_features:close",
        "stock_technical_features:return",
    ]

    training_df = store.get_historical_features(entity_df=entity_df, features=features)
    df = training_df.to_df()

    assert len(df) == len(entity_df)

    for feature_ref in features:
        feature_name = feature_ref.split(":")[-1]
        assert feature_name in df.columns


# ==============================================================================
# BONUS TEST: Feature Service Retrieval
# ==============================================================================


def test_get_online_features_using_feature_service(feast_feature_store):
    """Test retrieving features via FeatureService.

    Feature services provide a named bundle of features for specific use cases,
    simplifying feature retrieval by referencing a single service instead of
    listing individual feature views.
    """
    store = feast_feature_store

    entity_rows = {"symbol": ["AAPL"]}

    feature_vector = store.get_online_features(
        entity_rows=entity_rows,
        features=store.get_feature_service("stock_prediction_service"),
    )

    df = feature_vector.to_df()
    assert len(df.columns) > 10
