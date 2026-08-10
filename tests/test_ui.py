"""Tests for Stock Prediction UI components.

Covers pure logic (utils, historical helpers, plot builder)
and HTTP client behavior (api_client).
Streamlit rendering is NOT tested — side-effect heavy, low ROI.
"""

from datetime import datetime

import httpx
import pandas as pd
import plotly.graph_objects as go
import pytest

from stock_prediction_ml.config.settings import settings

# ── Fixtures ───────────────────────────────────────────────────────────
API_BASE_URL = f"http://{settings.api_host}:{settings.api_port}"


@pytest.fixture
def sample_records():
    """A reusable list of DailyRecord dicts for historical tests.

    Mix of predicted/unpredicted, correct/incorrect rows.
    """
    return [
        {
            "date": "2026-02-09",
            "close": 150.0,
            "actual_direction": "UP",
            "predicted_direction": "UP",
            "probability": 0.72,
            "correct": True,
        },
        {
            "date": "2026-02-10",
            "close": 148.0,
            "actual_direction": "DOWN",
            "predicted_direction": "DOWN",
            "probability": 0.65,
            "correct": True,
        },
        {
            "date": "2026-02-11",
            "close": 149.0,
            "actual_direction": "UP",
            "predicted_direction": "DOWN",
            "probability": 0.55,
            "correct": False,
        },
        {
            "date": "2026-02-12",
            "close": 151.0,
            "actual_direction": "UP",
            "predicted_direction": None,
            "probability": None,
            "correct": None,
        },
        {
            "date": "2026-02-13",
            "close": 152.0,
            "actual_direction": "UP",
            "predicted_direction": "UP",
            "probability": 0.80,
            "correct": True,
        },
    ]


@pytest.fixture
def chart_df(sample_records):
    """DataFrame shaped for build_price_chart (renamed columns)."""
    df = pd.DataFrame(sample_records).rename(
        columns={
            "date": "Date",
            "close": "Close",
            "predicted_direction": "Predicted",
            "correct": "Correct",
        }
    )
    return df


# ==================== UTILS — get_valid_symbols ====================


class TestGetValidSymbols:

    def test_returns_list(self):
        from stock_prediction_ml.ui.utils import get_valid_symbols

        symbols_list = get_valid_symbols()

        assert isinstance(symbols_list, list)

    def test_contains_strings(self):
        from stock_prediction_ml.ui.utils import get_valid_symbols

        symbols_list = get_valid_symbols()

        for symbol in symbols_list:
            assert isinstance(symbol, str)


# ==================== UTILS — get_next_trading_day ====================


class TestGetNextTradingDay:

    def test_weekday_returns_next_day(self, mocker):
        from stock_prediction_ml.ui.utils import get_next_trading_day

        mock_datetime = mocker.patch("stock_prediction_ml.ui.utils.datetime")
        mock_datetime.today.return_value.date.return_value = datetime(2026, 2, 16).date()

        date = get_next_trading_day()

        assert date == datetime(2026, 2, 17).date()

    def test_friday_skips_weekend(self, mocker):
        from stock_prediction_ml.ui.utils import get_next_trading_day

        mock_datetime = mocker.patch("stock_prediction_ml.ui.utils.datetime")
        mock_datetime.today.return_value.date.return_value = datetime(2026, 2, 13).date()

        date = get_next_trading_day()

        assert date == datetime(2026, 2, 16).date()

    def test_saturday_returns_monday(self, mocker):
        from stock_prediction_ml.ui.utils import get_next_trading_day

        mock_datetime = mocker.patch("stock_prediction_ml.ui.utils.datetime")
        mock_datetime.today.return_value.date.return_value = datetime(2026, 2, 14).date()

        date = get_next_trading_day()

        assert date == datetime(2026, 2, 16).date()

    def test_sunday_returns_monday(self, mocker):
        from stock_prediction_ml.ui.utils import get_next_trading_day

        mock_datetime = mocker.patch("stock_prediction_ml.ui.utils.datetime")
        mock_datetime.today.return_value.date.return_value = datetime(2026, 2, 15).date()

        date = get_next_trading_day()

        assert date == datetime(2026, 2, 16).date()


# ==================== UTILS — format_prediction_result ====================


class TestFormatPredictionResult:

    def test_up_prediction(self):
        from stock_prediction_ml.ui.utils import format_prediction_result

        result = format_prediction_result(
            {"prediction_label": "UP", "probability": 0.72}
        )

        assert result["direction"] == "UP"
        assert result["confidence"] == "72.00%"
        assert result["color"] == "green"
        assert result["emoji"] == "📈"

    def test_down_prediction(self):
        from stock_prediction_ml.ui.utils import format_prediction_result

        result = format_prediction_result(
            {"prediction_label": "DOWN", "probability": 0.65}
        )

        assert result["direction"] == "DOWN"
        assert result["confidence"] == "65.00%"
        assert result["color"] == "red"
        assert result["emoji"] == "📉"

    def test_fifty_percent_confidence(self):
        from stock_prediction_ml.ui.utils import format_prediction_result

        result = format_prediction_result(
            {"prediction_label": "UP", "probability": 0.50}
        )

        assert result["confidence"] == "50.00%"


# ==================== HISTORICAL — _compute_streak ====================


class TestComputeStreak:

    def test_trailing_correct_streak(self, sample_records):
        from stock_prediction_ml.ui.pages.historical import _compute_streak

        streak = _compute_streak(sample_records)

        assert streak == "1 correct"

    def test_all_correct(self):
        from stock_prediction_ml.ui.pages.historical import _compute_streak

        sample_all_true = [
            {"date": "2026-02-09", "predicted_direction": "UP", "correct": True},
            {"date": "2026-02-10", "predicted_direction": "UP", "correct": True},
            {"date": "2026-02-11", "predicted_direction": "DOWN", "correct": True},
            {"date": "2026-02-12", "predicted_direction": "UP", "correct": True},
            {"date": "2026-02-13", "predicted_direction": "UP", "correct": True},
        ]

        correct_streak_true = _compute_streak(sample_all_true)

        assert correct_streak_true == "5 correct"

    def test_all_incorrect(self):
        from stock_prediction_ml.ui.pages.historical import _compute_streak

        sample_all_false = [
            {"date": "2026-02-09", "predicted_direction": "UP", "correct": False},
            {"date": "2026-02-10", "predicted_direction": "DOWN", "correct": False},
            {"date": "2026-02-11", "predicted_direction": "UP", "correct": False},
            {"date": "2026-02-12", "predicted_direction": "DOWN", "correct": False},
            {"date": "2026-02-13", "predicted_direction": "UP", "correct": False},
        ]

        correct_streak_false = _compute_streak(sample_all_false)

        assert correct_streak_false == "5 incorrect"

    def test_empty_records(self):
        from stock_prediction_ml.ui.pages.historical import _compute_streak

        empty_record = []

        empty = _compute_streak(empty_record)

        assert empty == "N/A"

    def test_no_predicted_records(self):
        from stock_prediction_ml.ui.pages.historical import _compute_streak

        no_predicted_records = [
            {"date": "2026-02-09", "predicted_direction": None, "correct": None},
            {"date": "2026-02-10", "predicted_direction": None, "correct": None},
            {"date": "2026-02-11", "predicted_direction": None, "correct": None},
            {"date": "2026-02-12", "predicted_direction": None, "correct": None},
            {"date": "2026-02-13", "predicted_direction": None, "correct": None},
        ]

        empty = _compute_streak(no_predicted_records)

        assert empty == "N/A"


# ==================== HISTORICAL — _direction_precision ====================


class TestDirectionPrecision:

    def test_perfect_up_precision(self):
        from stock_prediction_ml.ui.pages.historical import _direction_precision

        sample_all_true = [
            {"date": "2026-02-09", "predicted_direction": "UP", "correct": True},
            {"date": "2026-02-10", "predicted_direction": "UP", "correct": True},
            {"date": "2026-02-11", "predicted_direction": "UP", "correct": True},
            {"date": "2026-02-12", "predicted_direction": "UP", "correct": True},
            {"date": "2026-02-13", "predicted_direction": "UP", "correct": True},
        ]

        hundred_precision = _direction_precision(sample_all_true, "UP")

        assert hundred_precision == "100.00%"

    def test_mixed_up_precision(self):
        from stock_prediction_ml.ui.pages.historical import _direction_precision

        sample_partial = [
            {"date": "2026-02-09", "predicted_direction": "UP", "correct": True},
            {"date": "2026-02-10", "predicted_direction": "UP", "correct": True},
            {"date": "2026-02-11", "predicted_direction": "UP", "correct": False},
            {"date": "2026-02-12", "predicted_direction": "UP", "correct": True},
            {"date": "2026-02-13", "predicted_direction": "UP", "correct": True},
        ]

        partial_precision = _direction_precision(sample_partial, "UP")

        assert partial_precision == "80.00%"

    def test_no_predictions_for_direction(self):
        from stock_prediction_ml.ui.pages.historical import _direction_precision

        sample_all_true = [
            {"date": "2026-02-09", "predicted_direction": "UP", "correct": True},
            {"date": "2026-02-10", "predicted_direction": "UP", "correct": True},
            {"date": "2026-02-11", "predicted_direction": "UP", "correct": True},
            {"date": "2026-02-12", "predicted_direction": "UP", "correct": True},
            {"date": "2026-02-13", "predicted_direction": "UP", "correct": True},
        ]

        no_result = _direction_precision(sample_all_true, "DOWN")

        assert no_result == "N/A"


# ==================== PLOT — build_price_chart ====================


class TestBuildPriceChart:

    def test_returns_figure(self, chart_df):
        from stock_prediction_ml.ui.components.plot import build_price_chart

        fig = build_price_chart(chart_df)

        assert isinstance(fig, go.Figure)

    def test_has_price_line_trace(self, chart_df):
        from stock_prediction_ml.ui.components.plot import build_price_chart

        fig = build_price_chart(chart_df)

        assert fig.data[0].name == "Close Price"

    def test_has_correct_markers(self, chart_df):
        from stock_prediction_ml.ui.components.plot import build_price_chart

        fig = build_price_chart(chart_df)
        trace_names = [t.name for t in fig.data]

        assert "Correct" in trace_names

    def test_has_incorrect_markers(self, chart_df):
        from stock_prediction_ml.ui.components.plot import build_price_chart

        fig = build_price_chart(chart_df)
        trace_names = [t.name for t in fig.data]

        assert "Incorrect" in trace_names

    def test_no_predictions_only_price_line(self):
        from stock_prediction_ml.ui.components.plot import build_price_chart

        no_predictions_df = pd.DataFrame(
            {
                "Date": ["2026-02-09", "2026-02-10"],
                "Close": [150.0, 148.0],
                "Predicted": [None, None],  # No predictions!
                "Correct": [None, None],
            }
        )

        fig = build_price_chart(no_predictions_df)

        assert len(fig.data) == 1  # Only price line


# ==================== API CLIENT ====================

class TestHealthCheck:

    def test_returns_dict_on_success(self, mocker):
        from stock_prediction_ml.ui.components.api_client import health_check

        mock_get = mocker.patch("httpx.get")
        mock_response = mock_get.return_value

        mock_response.status_code = 200
        mock_response.json.return_value = {
            "status": "healthy",
            "model_loaded": True,
            "feast_online_store": True,
            "model_version": "2",
        }

        result = health_check()

        assert result == {
            "status": "healthy",
            "model_loaded": True,
            "feast_online_store": True,
            "model_version": "2",
        }

        mock_get.assert_called_once_with(f"{API_BASE_URL}/health", timeout=5.0)

    def test_returns_none_on_connection_error(self, mocker):
        from stock_prediction_ml.ui.components.api_client import health_check

        mock_get = mocker.patch("httpx.get")
        mock_get.side_effect = httpx.ConnectError("No connection")

        result = health_check()

        assert result is None

    def test_returns_none_on_http_error(self, mocker):
        from stock_prediction_ml.ui.components.api_client import health_check

        mock_get = mocker.patch("httpx.get")
        mock_get.return_value.raise_for_status.side_effect = httpx.HTTPStatusError(
            "HTTP error", request=mocker.Mock(), response=mocker.Mock()
        )

        result = health_check()

        assert result is None


class TestPredict:

    def test_returns_prediction_dict(self, mocker):
        from stock_prediction_ml.ui.components.api_client import predict

        payload = {
            "symbol": "AAPL",
            "as_of_date": "2026-02-16",
            "target_date": "2026-02-17",
            "prediction": 1,
            "prediction_label": "UP",
            "probability": 0.75,
            "model_version": "2",
        }

        mock_post = mocker.patch("httpx.post")
        mock_response = mock_post.return_value

        mock_response.status_code = 200
        mock_response.json.return_value = payload

        result = predict("AAPL")

        assert result == payload
        mock_post.assert_called_once_with(
            f"{API_BASE_URL}/predict",
            json={"symbol": "AAPL"},
            timeout=10.0,
        )

    def test_returns_none_on_request_error(self, mocker):
        from stock_prediction_ml.ui.components.api_client import predict

        mock_post = mocker.patch("httpx.post")
        mock_post.side_effect = httpx.RequestError("Error request")

        result = predict("AAPL")

        assert result is None

    def test_returns_none_on_http_error(self, mocker):
        from stock_prediction_ml.ui.components.api_client import predict

        mock_post = mocker.patch("httpx.post")
        mock_post.return_value.raise_for_status.side_effect = httpx.HTTPStatusError(
            "HTTP error", request=mocker.Mock(), response=mocker.Mock()
        )

        result = predict("AAPL")

        assert result is None


class TestGetHistoricalData:

    def test_returns_data_on_success(self, mocker):
        from stock_prediction_ml.ui.components.api_client import get_historical_data

        mock_get = mocker.patch("httpx.get")
        mock_response = mock_get.return_value

        payload = {
            "data": [
                {
                    "date": "2026-02-09",
                    "close": 150.0,
                    "actual_direction": "UP",
                    "predicted_direction": "UP",
                    "probability": 0.72,
                    "correct": True,
                },
                {
                    "date": "2026-02-10",
                    "close": 148.0,
                    "actual_direction": "DOWN",
                    "predicted_direction": "DOWN",
                    "probability": 0.65,
                    "correct": True,
                },
                {
                    "date": "2026-02-11",
                    "close": 149.0,
                    "actual_direction": "UP",
                    "predicted_direction": "DOWN",
                    "probability": 0.55,
                    "correct": False,
                },
            ]
        }

        mock_response.status_code = 200
        mock_response.json.return_value = payload

        result = get_historical_data("AAPL", "2026-02-09", "2026-02-11")

        assert result == payload

    def test_sends_query_params(self, mocker):
        from stock_prediction_ml.ui.components.api_client import get_historical_data

        mock_get = mocker.patch("httpx.get")

        _ = get_historical_data("AAPL", "2026-02-09", "2026-02-11")

        mock_get.assert_called_once_with(
            f"{API_BASE_URL}/stock/history",
            params={
                "symbol": "AAPL",
                "start_date": "2026-02-09",
                "end_date": "2026-02-11",
            },
            timeout=10.0,
        )

    def test_returns_none_on_request_error(self, mocker):
        from stock_prediction_ml.ui.components.api_client import get_historical_data

        mock_get = mocker.patch("httpx.get")
        mock_get.side_effect = httpx.RequestError("Error request")

        result = get_historical_data("AAPL", "2026-02-09", "2026-02-11")

        assert result is None

    def test_returns_none_on_http_error(self, mocker):
        from stock_prediction_ml.ui.components.api_client import get_historical_data

        mock_get = mocker.patch("httpx.get")
        mock_get.return_value.raise_for_status.side_effect = httpx.HTTPStatusError(
            "HTTP error", request=mocker.Mock(), response=mocker.Mock()
        )

        result = get_historical_data("AAPL", "2026-02-09", "2026-02-11")

        assert result is None


class TestGetModelInfo:

    def test_returns_info_on_success(self, mocker):
        from stock_prediction_ml.ui.components.api_client import get_model_info

        mock_get = mocker.patch("httpx.get")
        mock_response = mock_get.return_value

        payload = {
            "model_version": "2",
            "model_name": "stock_prediction_classifier",
            "trained_on": "2026-02-15",
            "model_type": "CatBoost",
            "accuracy": 0.68,
            "precision": 0.71,
            "recall": 0.65,
            "f1_score": 0.68,
            "training_samples": 1250,
            "feature_count": 24,
            "classes": ["UP", "DOWN"],
        }

        mock_response.status_code = 200
        mock_response.json.return_value = payload

        result = get_model_info()

        assert result == payload
        mock_get.assert_called_once_with(f"{API_BASE_URL}/model/info", timeout=10.0)

    def test_returns_none_on_request_error(self, mocker):
        from stock_prediction_ml.ui.components.api_client import get_model_info

        mock_get = mocker.patch("httpx.get")
        mock_get.side_effect = httpx.RequestError("Error request")

        result = get_model_info()

        assert result is None

    def test_returns_none_on_http_error(self, mocker):
        from stock_prediction_ml.ui.components.api_client import get_model_info

        mock_get = mocker.patch("httpx.get")
        mock_get.return_value.raise_for_status.side_effect = httpx.HTTPStatusError(
            "HTTP error", request=mocker.Mock(), response=mocker.Mock()
        )

        result = get_model_info()

        assert result is None
