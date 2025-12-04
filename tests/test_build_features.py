import pandas as pd

from stock_prediction_ml.features.build_features import (
    create_features,
    read_combined_data,
    read_raw_data,
    save_combined_data,
    save_feature_data,
)


def test_read_raw_data_merges_all_parquet_files(tmp_path):
    # Create sample Parquet files
    df1 = pd.DataFrame(
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
    df2 = pd.DataFrame(
        {
            "date": ["2025-01-03", "2025-01-04"],
            "symbol": ["AAPL", "AAPL"],
            "open": [132.0, 133.0],
            "high": [134.0, 135.0],
            "low": [131.0, 132.0],
            "close": [133.0, 134.0],
            "volume": [1200000, 1300000],
            "adj_close": [133.0, 134.0],
        }
    )
    file1 = tmp_path / "data1.parquet"
    file2 = tmp_path / "data2.parquet"
    df1.to_parquet(file1, index=False)
    df2.to_parquet(file2, index=False)

    # Read raw data
    df_combined = read_raw_data(str(tmp_path))

    # Check if combined DataFrame is correct
    assert len(df_combined) == 4
    assert sorted(df_combined["date"].unique().tolist()) == [
        pd.Timestamp("2025-01-01", tz="UTC"),
        pd.Timestamp("2025-01-02", tz="UTC"),
        pd.Timestamp("2025-01-03", tz="UTC"),
        pd.Timestamp("2025-01-04", tz="UTC"),
    ]


def test_save_then_read_combined_data(tmp_path):
    df_original = pd.DataFrame(
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
    save_path = tmp_path / "combined.parquet"

    # Save combined data
    save_combined_data(df_original, str(save_path))

    # Read combined data
    df_loaded = read_combined_data(str(save_path))

    # Check if loaded DataFrame matches original
    pd.testing.assert_frame_equal(df_original, df_loaded)


def test_create_features():
    df = pd.DataFrame(
        {
            "date": ["2025-01-01", "2025-01-02", "2025-01-03"],
            "symbol": ["AAPL", "AAPL", "AAPL"],
            "open": [130.0, 131.0, 132.0],
            "high": [132.0, 133.0, 134.0],
            "low": [129.0, 130.0, 131.0],
            "close": [131.0, 132.0, 133.0],
            "volume": [1000000, 1100000, 1200000],
            "adj_close": [131.0, 132.0, 133.0],
        }
    )

    df_features = create_features(df)

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
        "sma_10"
    ]

    for column in expected_columns:
        assert column in df_features.columns

    assert df_features.dropna().shape[0] == 0
    assert df_features.index.is_monotonic_increasing


def test_save_feature_data_creates_valid_parquet(tmp_path):
    df = pd.DataFrame(
        {
            "date": ["2025-01-01", "2025-01-02"],
            "symbol": ["AAPL", "AAPL"],
            "open": [130.0, 131.0],
            "high": [132.0, 133.0],
            "low": [129.0, 130.0],
            "close": [131.0, 132.0],
            "volume": [1000000, 1100000],
            "adj_close": [131.0, 132.0],
            "target": [1, 0],
        }
    )
    save_path = tmp_path / "features.parquet"

    save_feature_data(df, str(save_path))
    assert save_path.exists()

    df_loaded = pd.read_parquet(save_path)
    pd.testing.assert_frame_equal(df, df_loaded)
