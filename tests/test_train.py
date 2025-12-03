import json
from pathlib import PosixPath

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
    load_raw_training_data,
    load_selected_features,
    save_model,
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
        "target": "target",
        "model_params": {"iterations": 50, "depth": 4, "random_seed": 42},
        "test_size": 0.2,
        "meta_dir": str(tmp_path / "meta"),
    }
    path = tmp_path / "config.yaml"
    path.write_text(yaml.dump(cfg))
    return path


@pytest.fixture
def raw_df(tmp_path, test_config_path):
    # Build a small synthetic DataFrame and save parquet
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
    df.to_parquet(tmp_path / "features.parquet")
    return load_raw_training_data(tmp_path / "features.parquet")


@pytest.fixture
def selected_features(tmp_path):
    # Save small feature list JSON
    feats = {"features": ["return", "symbol_AAPL", "symbol_MSFT", "symbol_TSLA"]}
    (tmp_path / "selected_features.json").write_text(json.dumps(feats))
    return load_selected_features(tmp_path / "selected_features.json")


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
def fitted_encoder(train_test_split, tmp_path):
    """Fit and save encoder, return tmp_path."""
    train, _ = train_test_split
    fit_and_save_encoder(train, meta_dir=tmp_path)
    return tmp_path


@pytest.fixture
def encoded_data(train_val_test_split, fitted_encoder):
    """Return encoded train, val, test DataFrames."""
    train, val, test = train_val_test_split

    train_encoded = load_and_transform_with_encoder(train, meta_dir=fitted_encoder)
    val_encoded = load_and_transform_with_encoder(val, meta_dir=fitted_encoder)
    test_encoded = load_and_transform_with_encoder(test, meta_dir=fitted_encoder)

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


@pytest.fixture
def saved_model_path(trained_model, tmp_path):
    """Save model once and return its Path."""
    model, _ = trained_model
    return save_model(model, tmp_path)


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


def test_load_raw_training_data_returns_training_dataframe(raw_df):
    assert isinstance(raw_df, pd.DataFrame)
    assert raw_df is not None
    assert not raw_df.empty


def test_load_raw_training_data_has_date_column_as_datetime(raw_df):
    """Should convert 'date' column to datetime type."""
    assert pd.api.types.is_datetime64_any_dtype(raw_df["date"])


def test_load_raw_training_data_is_sorted_by_symbol_and_date(raw_df):
    """Should return data sorted by ['symbol', 'date']."""
    sorted_df = raw_df.sort_values(by=["symbol", "date"])
    pd.testing.assert_frame_equal(raw_df, sorted_df)


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


def test_fit_and_save_encoder_creates_pkl_file(fitted_encoder):
    """Should create 'ohe.pkl' file in the specified meta_dir."""
    pkl_file = fitted_encoder / "ohe.pkl"
    assert pkl_file.exists()

    with open(pkl_file, "rb") as f:
        encoder = joblib.load(f)

    assert isinstance(encoder, OneHotEncoder)


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


def test_save_model_creates_cbm_file(saved_model_path):
    """Should create a .cbm model file in the specified directory."""
    assert saved_model_path.exists()


def test_save_model_returns_path_object(saved_model_path):
    """Should return a Path object pointing to the saved model."""
    assert isinstance(saved_model_path, PosixPath)


def test_save_model_file_is_not_empty(saved_model_path):
    """Saved model file should have non-zero size."""
    assert saved_model_path.stat().st_size > 0


def test_save_model_file_can_be_loaded(saved_model_path):
    """Saved model should be loadable with CatBoost.load_model()."""
    model = CatBoostClassifier()
    model.load_model(saved_model_path)
    assert isinstance(model, CatBoostClassifier)
    assert getattr(model, "get_best_iteration", None) is not None


def test_save_model_with_custom_name(trained_model, tmp_path):
    """Should save model with custom name (e.g., 'my_model.cbm')."""
    model, _ = trained_model
    model_path = save_model(model, tmp_path, "custom_name")

    assert model_path.exists()
    assert str(model_path).endswith("custom_name.cbm")
