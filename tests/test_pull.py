import pytest
from stock_prediction_ml.marketstack.pull import *


def test_load_config_output_api_key():
    api_key = load_config()
    assert isinstance(api_key, str)


def test_fetch_eod_with_date_return_dict():
    api_key = load_config()
    data = fetch_eod_with_date(api_key, "AAPL", "2025-01-01", "2025-01-02")
    assert isinstance(data, dict)
    assert "data" in data
    assert isinstance(data["data"], list)


@pytest.mark.parametrize(
    "column",
    ["date", "close", "volume", "adjusted_close", "symbol", "open", "high", "low"],
)
def test_process_dataframe_output_dataframe_with_correct_column(column):
    api_key = load_config()
    data = fetch_eod_with_date(api_key, "AAPL", "2025-01-01", "2025-01-02")
    df = process_dataframe(data)
    assert isinstance(df, pd.DataFrame)
    assert column in df.columns


def test_save_to_parquet_creates_file(tmp_path):
    api_key = load_config()
    data = fetch_eod_with_date(api_key, "AAPL", "2025-01-01", "2025-01-02")
    df = process_dataframe(data)
    file_path = tmp_path / "test.parquet"
    save_to_parquet(df, file_path)
    assert file_path.exists(), "Parquet file was not created."
    loaded_df = pd.read_parquet(file_path)
    pd.testing.assert_frame_equal(df, loaded_df)
