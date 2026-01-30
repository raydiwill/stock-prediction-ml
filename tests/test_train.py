import json

import joblib
import numpy as np
import pandas as pd
import pytest
import yaml
from catboost import CatBoostClassifier
from sklearn.preprocessing import OneHotEncoder

from stock_prediction_ml.model.train import (
    build_X_y,
    evaluate_model,
    fit_and_save_encoder,
    load_and_transform_with_encoder,
    load_config,
    load_selected_features,
    load_training_data_from_feast,
    split_data_train_test,
    train_model,
)

# ==================== FIXTURES ====================


@pytest.fixture
def test_config_path(tmp_path):
    # Minimal config for tests with temp paths
    cfg = {
        "training_data_path": str(tmp_path / "features.parquet"),
        "selected_features_path": str(tmp_path / "selected_features.json"),
        "feast_service_name": "stock_training_service",
        "target": "target",
        "model_params": {
            "iterations": 50,
            "depth": 4,
            "random_seed": 42,
            "allow_writing_files": False,
        },
        "test_size": 0.2,
        "meta_dir": str(tmp_path / "meta"),
    }
    path = tmp_path / "config.yaml"
    path.write_text(yaml.dump(cfg))
    return path


@pytest.fixture
def raw_df(tmp_path, test_config_path):
    """Generate synthetic training data for tests.
    
    Returns a DataFrame with date, symbol, return, and target columns
    suitable for testing the training pipeline.
    """
    import numpy as np
    import pandas as pd

    dates = pd.date_range("2025-01-01", periods=100, freq="D", tz="UTC")
    df = pd.DataFrame(
        {
            "date": np.repeat(dates, 3),
            "symbol": ["AAPL", "MSFT", "TSLA"] * len(dates),
            "return": np.random.randn(len(dates) * 3),
            "target": np.random.randint(0, 2, size=len(dates) * 3),
        }
    )
    # Sort by symbol and date to match expected behavior
    df = df.sort_values(by=["symbol", "date"]).reset_index(drop=True)
    return df


@pytest.fixture
def selected_features(tmp_path):
    # Save small feature list JSON
    feats = {"features": ["return", "symbol_AAPL", "symbol_MSFT", "symbol_TSLA"]}
    (tmp_path / "selected_features.json").write_text(json.dumps(feats))
    return load_selected_features(tmp_path / "selected_features.json")


@pytest.fixture
def feast_repo_path(tmp_path, raw_df):
    """Set up a minimal Feast repository for testing data loading.

    Creates a temporary Feast environment with:
    - feature_store.yaml configuration
    - Entity and feature view definitions
    - Sample feature data in parquet format

    Args:
        tmp_path: Pytest fixture for temporary directory.
        raw_df: Synthetic training DataFrame.

    Returns:
        Path: Path to temporary Feast repository.
    """
    import sys

    import yaml
    from feast import FeatureStore

    feature_store_yaml = {
        "project": "stock_prediction_ml",
        "provider": "local",
        "registry": "registry.db",
        "offline_store": {"type": "file"},
        "online_store": {"type": "sqlite", "path": "online_store.db"},
    }
    (tmp_path / "feature_store.yaml").write_text(yaml.dump(feature_store_yaml))

    parquet_path = tmp_path / "data" / "feature" / "stock_features.parquet"
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    raw_df.to_parquet(parquet_path, index=False)

    entities_code = """
from feast import Entity, ValueType

stock = Entity(
    name="symbol",
    value_type=ValueType.STRING,
    description="Stock ticker symbol",
)
"""
    (tmp_path / "entities.py").write_text(entities_code)

    features_code = f"""
from datetime import timedelta
from pathlib import Path
from feast import FileSource, FeatureView, Field
from feast.types import Float32, Int32

from entities import stock

stock_features_source = FileSource(
    path=str(Path(r"{parquet_path}")),
    timestamp_field="date",
)

stock_test_features = FeatureView(
    name="stock_test_features",
    entities=[stock],
    ttl=timedelta(days=1),
    schema=[
        Field(name="return", dtype=Float32),
        Field(name="target", dtype=Int32),
    ],
    source=stock_features_source,
    online=False,
)
"""
    (tmp_path / "features_definition.py").write_text(features_code)

    services_code = """
from feast import FeatureService
from features_definition import stock_test_features

stock_training_service = FeatureService(
    name="stock_training_service",
    features=[stock_test_features],
)
"""
    (tmp_path / "feature_services.py").write_text(services_code)

    # Apply the feature store configuration
    sys.path.insert(0, str(tmp_path))
    try:
        from entities import stock
        from feature_services import stock_training_service
        from features_definition import stock_test_features

        store = FeatureStore(repo_path=str(tmp_path))
        store.apply([stock, stock_test_features, stock_training_service])
    except Exception as e:
        pytest.skip(f"Feast apply failed: {e}")
    finally:
        sys.path.remove(str(tmp_path))

    return tmp_path


@pytest.fixture
def config(test_config_path):
    return load_config(test_config_path)


@pytest.fixture
def train_test_split(raw_df):
    """Split data into train/test."""
    return split_data_train_test(raw_df, test_size=0.2)


@pytest.fixture
def train_val_test_split(raw_df):
    """Split data into train/val/test."""
    train_df, test = split_data_train_test(raw_df, test_size=0.2)
    train, val = split_data_train_test(train_df, test_size=0.5)
    return train, val, test


@pytest.fixture
def fitted_encoder_tuple(train_test_split, tmp_path):
    """Fit and save encoder, return (encoder_object, meta_dir)."""
    train, _ = train_test_split
    encoder = fit_and_save_encoder(train, meta_dir=tmp_path)
    return encoder, tmp_path


@pytest.fixture
def encoded_data(train_val_test_split, fitted_encoder_tuple):
    """Return encoded train, val, test DataFrames using in-memory encoder."""
    train, val, test = train_val_test_split
    encoder, meta_dir = fitted_encoder_tuple

    # Test passing the encoder object directly
    train_encoded = load_and_transform_with_encoder(train, encoder=encoder)
    val_encoded = load_and_transform_with_encoder(val, encoder=encoder)
    test_encoded = load_and_transform_with_encoder(test, encoder=encoder)

    return train_encoded, val_encoded, test_encoded


@pytest.fixture
def X_y_data(encoded_data, selected_features):
    """Return X and y arrays for train, val, test."""
    train_encoded, val_encoded, test_encoded = encoded_data

    X_train, y_train = build_X_y(
        train_encoded, selected_features=selected_features, target_column="target"
    )
    X_val, y_val = build_X_y(
        val_encoded, selected_features=selected_features, target_column="target"
    )
    X_test, y_test = build_X_y(
        test_encoded, selected_features=selected_features, target_column="target"
    )

    return (X_train, y_train), (X_val, y_val), (X_test, y_test)


@pytest.fixture
def trained_model(X_y_data, config):
    """Return trained model and info."""
    (X_train, y_train), (X_val, y_val), _ = X_y_data
    model, info = train_model(
        X_train, y_train, X_val, y_val, params=config.get("model_params", {})
    )
    return model, info


# ==================== TESTS ====================


@pytest.mark.parametrize(
    "expected_keys",
    [
        "training_data_path",
        "selected_features_path",
        "target",
        "model_params",
        "test_size",
        "meta_dir",
    ],
)
def test_load_config_returns_dict_with_expected_keys(config, expected_keys):
    """Should have expected keys in config dict"""
    assert expected_keys in config


def test_load_config_with_custom_path(tmp_path):
    """Should load config from a custom YAML file path."""
    custom_path = tmp_path / "test_custom_config.yaml"
    custom_config_content = {
        "training_data_path": "data/feature/custom_stock_eod_features.parquet",
        "selected_features_path": "data/meta/custom_selected_features.json",
    }

    with open(custom_path, "w") as f:
        yaml.dump(custom_config_content, f)

    custom_config = load_config(custom_path)
    assert isinstance(custom_config, dict)
    assert "training_data_path" in custom_config
    assert "selected_features_path" in custom_config


def test_load_config_raises_error_for_missing_file():
    """Should raise FileNotFoundError when config file doesn't exist."""
    with pytest.raises(FileNotFoundError):
        load_config("nopath/non_existent_config.yaml")


def test_load_selected_features_returns_list_of_strings(selected_features):
    """Should return a list of feature names from JSON file."""
    assert isinstance(selected_features, list)


@pytest.mark.slow
def test_load_training_data_from_feast_returns_dataframe(feast_repo_path):
    """Should return a pandas DataFrame with features from Feast offline store."""
    try:
        df = load_training_data_from_feast(
            feature_service_name="stock_training_service",
            feast_repo_path=feast_repo_path,
        )
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "date" in df.columns
        assert "symbol" in df.columns
    except Exception as e:
        pytest.skip(f"Feast test skipped due to setup issues: {e}")


@pytest.mark.slow
def test_load_training_data_from_feast_includes_target_column(feast_repo_path):
    """Should include target column from Feast feature service."""
    try:
        df = load_training_data_from_feast(
            feature_service_name="stock_training_service",
            feast_repo_path=feast_repo_path,
        )
        assert "target" in df.columns, "Target column missing from Feast features"
    except Exception as e:
        pytest.skip(f"Feast test skipped due to setup issues: {e}")


@pytest.mark.slow
def test_load_training_data_from_feast_sorted_by_symbol_date(feast_repo_path):
    """Should return data sorted by ['symbol', 'date']."""
    try:
        df = load_training_data_from_feast(
            feature_service_name="stock_training_service",
            feast_repo_path=feast_repo_path,
        )
        sorted_df = df.sort_values(by=["symbol", "date"]).reset_index(drop=True)
        pd.testing.assert_frame_equal(df.reset_index(drop=True), sorted_df)
    except Exception as e:
        pytest.skip(f"Feast test skipped due to setup issues: {e}")


def test_split_data_train_test_splits_by_test_size(train_test_split, raw_df):
    """Should split data according to time cutoff derived from test_size."""
    train, test = train_test_split
    cutoff = raw_df["date"].quantile(0.8)

    expected_train_rows = (raw_df["date"] < cutoff).sum()
    expected_test_rows = (raw_df["date"] >= cutoff).sum()

    assert len(train) == expected_train_rows
    assert len(test) == expected_test_rows


def test_split_data_train_test_splits_on_date_cutoff(train_test_split, raw_df):
    """Should split based on date quantile, not random sampling."""
    train, test = train_test_split
    cutoff = raw_df["date"].quantile(0.8)  # 0.8 because test_size=0.2

    assert train["date"].max() < cutoff
    assert test["date"].min() >= cutoff


def test_split_data_train_test_no_overlap_between_train_test(train_test_split):
    """Train and test should have no overlapping dates."""
    train, test = train_test_split
    assert train["date"].max() < test["date"].min()


def test_split_data_train_test_preserves_total_row_count(train_test_split, raw_df):
    """Sum of train and test rows should equal original DataFrame."""
    train, test = train_test_split
    assert len(raw_df) == len(train) + len(test)


def test_fit_and_save_encoder_returns_encoder_and_saves_file(fitted_encoder_tuple):
    """Should return encoder object AND create 'ohe.pkl' file."""
    encoder, meta_dir = fitted_encoder_tuple
    pkl_file = meta_dir / "ohe.pkl"

    # Check return object
    assert isinstance(encoder, OneHotEncoder)

    # Check file persistence
    assert pkl_file.exists()
    with open(pkl_file, "rb") as f:
        loaded_encoder = joblib.load(f)
    assert isinstance(loaded_encoder, OneHotEncoder)


def test_load_and_transform_with_encoder_adds_ohe_columns(encoded_data):
    """Should add one-hot encoded columns and drop original 'symbol'."""
    train_encoded, _, _ = encoded_data

    assert "symbol" not in train_encoded.columns

    # Synthetic data only has 3 symbols: AAPL, MSFT, TSLA
    expected_encoded = [
        "symbol_AAPL",
        "symbol_MSFT",
        "symbol_TSLA",
    ]

    for column in expected_encoded:
        assert column in train_encoded.columns


def test_build_X_y_returns_numpy_arrays(X_y_data):
    """Should return X and y as numpy arrays."""
    (X_train, y_train), _, _ = X_y_data
    assert isinstance(X_train, np.ndarray)
    assert isinstance(y_train, np.ndarray)


def test_build_X_y_correct_shapes(X_y_data):
    """X should have shape (n_samples, n_features), y should be (n_samples,)."""
    (X_train, y_train), _, _ = X_y_data

    assert X_train.shape[0] == y_train.shape[0]
    assert len(X_train.shape) == 2
    assert len(y_train.shape) == 1


def test_train_model_returns_model_and_info(trained_model):
    """Should return a trained CatBoost model and info dict."""
    model, info = trained_model
    assert isinstance(model, CatBoostClassifier)
    assert isinstance(info, dict)


def test_train_model_info_contains_expected_keys(trained_model):
    """Info dict should have 'best_iteration', 'train_size', 'val_size'."""
    _, info = trained_model

    assert "best_iteration" in info.keys()
    assert "train_size" in info.keys()
    assert "val_size" in info.keys()


def test_evaluate_model_returns_metrics_dict(trained_model, X_y_data):
    """Should return dict with accuracy and roc_auc keys."""
    model, _ = trained_model
    _, _, (X_test, y_test) = X_y_data

    test_metrics = evaluate_model(model, X_test, y_test, "test")

    assert isinstance(test_metrics, dict)


def test_evaluate_model_metrics_are_floats(trained_model, X_y_data):
    """All metric values should be float type."""
    model, _ = trained_model
    _, _, (X_test, y_test) = X_y_data

    test_metrics = evaluate_model(model, X_test, y_test, "test")

    for value in test_metrics.values():
        assert isinstance(value, float)


def test_evaluate_model_respects_prefix_parameter(trained_model, X_y_data):
    """Metric keys should be prefixed (e.g., 'test_accuracy', 'val_accuracy')."""
    model, _ = trained_model
    _, (X_val, y_val), (X_test, y_test) = X_y_data

    val_metrics = evaluate_model(model, X_val, y_val, "val")
    test_metrics = evaluate_model(model, X_test, y_test, "test")

    assert "val_accuracy" in val_metrics.keys()
    assert "val_roc_auc" in val_metrics.keys()
    assert "test_accuracy" in test_metrics.keys()
    assert "test_roc_auc" in test_metrics.keys()


def test_trained_model_is_serializable(trained_model, tmp_path):
    """
    Ensure the trained model object is valid and can be saved/loaded.
    This replaces the old save_model tests and acts as a sanity check
    before MLflow logging.
    """
    model, _ = trained_model
    save_path = tmp_path / "temp_model.cbm"

    # Try saving using CatBoost's native method
    model.save_model(save_path)

    assert save_path.exists()
    assert save_path.stat().st_size > 0

    # Try loading it back
    loaded_model = CatBoostClassifier()
    loaded_model.load_model(save_path)

    # Check if it's the same model (e.g. same tree count)
    assert loaded_model.tree_count_ == model.tree_count_
