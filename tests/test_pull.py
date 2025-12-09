from unittest.mock import Mock, patch

import pandas as pd
import pytest

from stock_prediction_ml.marketstack.pull import (
    combine_and_save_to_parquet,
    fetch_ticker_data,
    process_dataframe,
    save_to_parquet,
)


# ========== Fixtures ==========
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
    # Simulate pagination: total=1 means loop breaks after first batch
    mock_response.json.return_value = {
        "data": fake_stock_data,
        "pagination": {"total": 1},
    }
    return mock_response


# ========== Tests ==========
@patch("stock_prediction_ml.marketstack.pull.requests.get")
def test_fetch_ticker_data_invokes_api(mock_get, mock_api_response, fake_stock_data):
    """Test that fetch_ticker_data calls the API and returns a list."""
    mock_get.return_value = mock_api_response

    api_key = "dummy_key"
    data = fetch_ticker_data(api_key, "AAPL", "2025-01-03", "2025-01-10")

    mock_get.assert_called()
    assert isinstance(data, list)
    assert data == fake_stock_data


@patch("stock_prediction_ml.marketstack.pull.requests.get")
def test_fetch_ticker_data_pagination(mock_get):
    """Test that pagination loop works (calls API multiple times)."""
    # Setup: 2 pages of data
    page1 = {
        "data": [{"symbol": "AAPL", "date": "2024-01-01"}],
        "pagination": {"total": 2},
    }
    page2 = {
        "data": [{"symbol": "AAPL", "date": "2024-01-02"}],
        "pagination": {"total": 2},
    }

    # Mock responses for consecutive calls
    resp1 = Mock(status_code=200)
    resp1.json.return_value = page1

    resp2 = Mock(status_code=200)
    resp2.json.return_value = page2

    mock_get.side_effect = [resp1, resp2]

    data = fetch_ticker_data("key", "AAPL", "2024-01-01", "2024-01-02", limit=1)

    assert len(data) == 2
    assert mock_get.call_count == 2


@pytest.mark.parametrize(
    "column",
    [
        "date",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "adj_close",
        "pulled_at",
        "source",
    ],
)
def test_process_dataframe_columns_exist(fake_stock_data, column):
    """Test that processed dataframe has all required columns."""
    df = process_dataframe(fake_stock_data)

    assert isinstance(df, pd.DataFrame)
    assert column in df.columns
    if column == "source":
        assert df["source"].iloc[0] == "marketstack_api"


def test_process_dataframe_empty_input():
    """Test handling of empty input list."""
    df = process_dataframe([])
    assert df.empty


def test_save_to_parquet_writes_file(fake_stock_data, tmp_path, mocker):
    """Test saving dataframe to parquet."""
    df = process_dataframe(fake_stock_data)
    filename = "test.parquet"

    # Mock PROJECT_ROOT to point to tmp_path
    mocker.patch("stock_prediction_ml.marketstack.pull.PROJECT_ROOT", tmp_path)

    save_to_parquet(df, filename)

    expected_path = tmp_path / "data" / "raw" / filename
    assert expected_path.exists()

    loaded = pd.read_parquet(expected_path)
    # Reset index because parquet doesn't save index by default in our script
    pd.testing.assert_frame_equal(df, loaded)


def test_combine_and_save_to_parquet(tmp_path, mocker):
    """Test combining multiple parquet files."""
    # 1. Setup fake data directory
    raw_dir = tmp_path / "data" / "raw"
    raw_dir.mkdir(parents=True)

    df1 = pd.DataFrame({"symbol": ["AAPL"], "close": [100]})
    df2 = pd.DataFrame({"symbol": ["MSFT"], "close": [200]})

    df1.to_parquet(raw_dir / "AAPL.parquet")
    df2.to_parquet(raw_dir / "MSFT.parquet")

    # 2. Mock PROJECT_ROOT
    mocker.patch("stock_prediction_ml.marketstack.pull.PROJECT_ROOT", tmp_path)

    # 3. Run combine function
    combine_and_save_to_parquet()

    # 4. Assert output exists
    output_path = tmp_path / "data" / "processed" / "combined_eod.parquet"
    assert output_path.exists()

    combined_df = pd.read_parquet(output_path)
    assert len(combined_df) == 2
    assert "AAPL" in combined_df["symbol"].values
    assert "MSFT" in combined_df["symbol"].values
