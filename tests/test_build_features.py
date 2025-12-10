import pandas as pd
import pytest

from stock_prediction_ml.features.build_features import (
    create_features,
    read_validated_data,
    save_feature_data,
)


@pytest.fixture
def sample_dataframe():
    return pd.DataFrame(
        {
            "date": ["2025-01-01", "2025-01-02"],
            "symbol": ["AAPL", "AAPL"],
            "open": [130.0, 131.0],
            "high": [132.0, 133.0],
            "low": [129.0, 130.0],
            "close": [131.0, 132.0],
            "volume": [1000000, 1100000],
            "adj_close": [131.0, 132.0],
        }
    )


def test_read_validated_data_reads_parquet(sample_dataframe, tmp_path):
    file_path = tmp_path / "validated_data.parquet"
    sample_dataframe.to_parquet(file_path, index=False)

    # Read validated data
    df_loaded = read_validated_data(str(file_path))

    # Check if loaded DataFrame matches original (ignoring timezone for simplicity in test setup)
    # Note: read_validated_data converts date to UTC, so we check values
    assert len(df_loaded) == 2
    assert df_loaded["symbol"].tolist() == ["AAPL", "AAPL"]
    assert df_loaded["close"].tolist() == [131.0, 132.0]


def test_create_features(sample_dataframe):
    df_features = create_features(sample_dataframe)

    # Check if features are created
    expected_columns = [
        "target",
        "day_of_month",
        "return_lag_1",
        "return",
        "return_roll_mean_10",
        "return_roll_std_10",
        "return_roll_std_5",
        "return_roll_mean_5",
        "return_lag_10",
        "volume",
        "close_open",
        "high_low",
        "return_lag_2",
        "rsi_14",
        "return_lag_5",
        "adj_close",
        "macd_signal",
        "day_of_week",
        "month",
        "macd",
        "sma_10",
    ]

    for column in expected_columns:
        assert column in df_features.columns

    assert df_features.dropna().shape[0] == 0
    assert df_features.index.is_monotonic_increasing


def test_save_feature_data_creates_valid_parquet(sample_dataframe, mocker, tmp_path):
    mocker.patch("stock_prediction_ml.features.build_features.PROJECT_ROOT", tmp_path)

    save_feature_data(sample_dataframe, "test_features.parquet")

    expected_path = tmp_path / "data" / "feature" / "test_features.parquet"
    assert expected_path.exists()

    df_loaded = pd.read_parquet(expected_path)
    pd.testing.assert_frame_equal(sample_dataframe, df_loaded)
