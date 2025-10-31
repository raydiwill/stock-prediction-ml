from unittest.mock import Mock, patch

import pandas as pd
import pytest

from stock_prediction_ml.marketstack.pull import (
    fetch_eod_with_date,
    load_config,
    process_dataframe,
    save_to_parquet,
)


@pytest.fixture(autouse=True)
def set_env_api_key(monkeypatch):
    monkeypatch.setenv("MARKETSTACK_API_KEY", "dummy_key")


@pytest.fixture
def fake_stock_data():
    return [
        {
            "date": "2025-01-03",
            "symbol": "AAPL",
            "open": 130.0,
            "high": 131.0,
            "low": 129.0,
            "close": 130.5,
            "volume": 1000000,
            "adj_close": 130.5,
        }
    ]


@pytest.fixture
def mock_api_response(fake_stock_data):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": fake_stock_data}
    return mock_response


def test_load_config_returns_string():
    api_key = load_config()
    assert isinstance(api_key, str)
    assert api_key == "dummy_key"


@patch('stock_prediction_ml.marketstack.pull.requests.get')
def test_fetch_eod_with_date_invokes_api(mock_get, mock_api_response):
    mock_get.return_value = mock_api_response
    api_key = load_config()
    dates = ("2025-01-03", "2025-01-10")
    data = fetch_eod_with_date(api_key, ["AAPL"], *dates)
    mock_get.assert_called_once()
    assert isinstance(data, list)
    assert data == mock_api_response.json.return_value["data"]


@patch('stock_prediction_ml.marketstack.pull.requests.get')
@pytest.mark.parametrize(
    "column", ["date", "symbol", "open", "high", "low", "close", "volume", "adj_close"]
)
def test_process_dataframe_columns_exist(mock_get, mock_api_response, column):
    mock_get.return_value = mock_api_response
    api_key = load_config()
    data = fetch_eod_with_date(api_key, ["AAPL"], "2025-01-03", "2025-01-10")
    df = process_dataframe(data)
    assert isinstance(df, pd.DataFrame)
    assert column in df.columns


@patch('stock_prediction_ml.marketstack.pull.requests.get')
def test_save_to_parquet_writes_file(mock_get, mock_api_response, tmp_path):
    mock_get.return_value = mock_api_response
    api_key = load_config()
    data = fetch_eod_with_date(api_key, ["AAPL"], "2025-01-03", "2025-01-10")
    df = process_dataframe(data)
    out_file = tmp_path / "test.parquet"
    save_to_parquet(df, str(out_file))
    assert out_file.exists()
    loaded = pd.read_parquet(out_file)
    pd.testing.assert_frame_equal(df.reset_index(drop=True), loaded.reset_index(drop=True))
