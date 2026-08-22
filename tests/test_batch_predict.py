"""Tests for batch_predict module.

Strategy: mock external dependencies (MLflow, Feast, DB) so tests run
fast and hermetically. Each extracted function gets its own test group.
"""

import pandas as pd
import pytest

from stock_prediction_ml.db.models import RawStockData
from stock_prediction_ml.model.batch_predict import (
    fetch_features,
    load_champion_model,
    main,
    persist_results,
    run_predictions,
)

# ==================== FIXTURES ====================


@pytest.fixture
def sample_features_df():
    """Cleaned feature DataFrame as returned by fetch_features()."""
    return pd.DataFrame(
        {
            "symbol": ["AAPL", "MSFT"],
            "sma_20": [150.0, 310.0],
            "rsi_14": [55.0, 62.0],
            "day_of_week": pd.array([2, 3], dtype="int32"),
            "month": pd.array([3, 3], dtype="int32"),
        }
    )


@pytest.fixture
def sample_result_df(sample_features_df):
    """Feature DataFrame merged with prediction columns."""
    preds = pd.DataFrame(
        {
            "prediction_class": [1, 0],
            "prediction_proba_up": [0.72, 0.35],
            "prediction_proba_down": [0.28, 0.65],
        }
    )
    return pd.concat([sample_features_df, preds], axis=1)


@pytest.fixture
def mock_pyfunc_model(mocker):
    """A mock MLflow pyfunc model whose .predict() returns prediction columns."""
    mock = mocker.Mock()
    mock.predict.return_value = pd.DataFrame(
        {
            "prediction_class": [1, 0],
            "prediction_proba_up": [0.72, 0.35],
            "prediction_proba_down": [0.28, 0.65],
        }
    )
    return mock


# ==================== load_champion_model ====================


class TestLoadChampionModel:
    def test_returns_model_and_version(self, mocker):
        mocker.patch("stock_prediction_ml.model.batch_predict.mlflow.set_tracking_uri")

        mock_client = mocker.Mock()
        mock_client.get_model_version_by_alias.return_value.version = "3"
        mocker.patch(
            "stock_prediction_ml.model.batch_predict.MlflowClient",
            return_value=mock_client,
        )

        mock_model = mocker.Mock()
        mocker.patch(
            "stock_prediction_ml.model.batch_predict.mlflow.pyfunc.load_model",
            return_value=mock_model,
        )

        model, version = load_champion_model()

        assert model is mock_model
        assert version == "3"

    def test_version_falls_back_to_unknown_on_alias_error(self, mocker):
        mocker.patch("stock_prediction_ml.model.batch_predict.mlflow.set_tracking_uri")

        mock_client = mocker.Mock()
        mock_client.get_model_version_by_alias.side_effect = Exception("Model error!")
        mocker.patch(
            "stock_prediction_ml.model.batch_predict.MlflowClient",
            return_value=mock_client,
        )

        mock_model = mocker.Mock()
        mocker.patch(
            "stock_prediction_ml.model.batch_predict.mlflow.pyfunc.load_model",
            return_value=mock_model,
        )

        _, version = load_champion_model()

        assert version == "unknown"

    def test_raises_when_model_load_fails(self, mocker):
        mocker.patch("stock_prediction_ml.model.batch_predict.mlflow.set_tracking_uri")

        mock_client = mocker.Mock()
        mock_client.get_model_version_by_alias.return_value.version = "3"
        mocker.patch(
            "stock_prediction_ml.model.batch_predict.MlflowClient",
            return_value=mock_client,
        )

        mocker.patch(
            "stock_prediction_ml.model.batch_predict.mlflow.pyfunc.load_model",
            side_effect=Exception("Load failed!"),
        )

        with pytest.raises(Exception, match="Load failed"):
            load_champion_model()


# ==================== fetch_features ====================


class TestFetchFeatures:
    def test_returns_dataframe_with_expected_columns(self, mocker, sample_features_df):
        mock_store = mocker.patch(
            "stock_prediction_ml.model.batch_predict.FeatureStore"
        ).return_value
        mock_store.get_online_features.return_value.to_df.return_value = sample_features_df

        df = fetch_features(["AAPL", "MSFT"])
        expected = ["symbol", "sma_20", "rsi_14", "day_of_week", "month"]
        assert list(df.columns) == expected

    def test_drops_rows_with_nan_features(self, mocker):
        sample_with_nan = pd.DataFrame(
            {
                "symbol": ["AAPL", "MSFT"],
                "sma_20": [150.0, float("nan")],
                "rsi_14": [55.0, 62.0],
                "day_of_week": pd.array([2, 3], dtype="int32"),
                "month": pd.array([3, 3], dtype="int32"),
            }
        )

        mock_store = mocker.patch(
            "stock_prediction_ml.model.batch_predict.FeatureStore"
        ).return_value
        mock_store.get_online_features.return_value.to_df.return_value = sample_with_nan

        df = fetch_features(["AAPL", "MSFT"])
        assert len(df) == 1
        assert "MSFT" not in df.symbol

    def test_raises_value_error_when_all_rows_nan(self, mocker):
        sample_all_nan = pd.DataFrame(
            {
                "symbol": ["AAPL", "MSFT"],
                "sma_20": [float("nan"), float("nan")],
            }
        )
        mock_store = mocker.patch(
            "stock_prediction_ml.model.batch_predict.FeatureStore"
        ).return_value
        mock_store.get_online_features.return_value.to_df.return_value = sample_all_nan

        with pytest.raises(ValueError):
            fetch_features(["AAPL", "MSFT"])

    def test_casts_int_columns_to_int32(self, mocker):
        import numpy as np

        int64_df = pd.DataFrame(
            {
                "symbol": ["AAPL", "MSFT"],
                "sma_20": [150.0, 310.0],
                "rsi_14": [55.0, 62.0],
                "day_of_week": pd.array([2, 3], dtype="int64"),
                "month": pd.array([3, 3], dtype="int64"),
            }
        )

        mock_store = mocker.patch(
            "stock_prediction_ml.model.batch_predict.FeatureStore"
        ).return_value
        mock_store.get_online_features.return_value.to_df.return_value = int64_df

        df = fetch_features(["AAPL", "MSFT"])

        for col in ["day_of_week", "month"]:
            assert df[col].dtype == np.int32


# ==================== run_predictions ====================


class TestRunPredictions:
    def test_returns_merged_dataframe(
        self, mock_pyfunc_model, sample_result_df, sample_features_df
    ):
        df = run_predictions(mock_pyfunc_model, sample_features_df)

        pd.testing.assert_frame_equal(df, sample_result_df)

    def test_row_count_preserved(self, mock_pyfunc_model, sample_features_df):
        df = run_predictions(mock_pyfunc_model, sample_features_df)

        assert len(df) == 2


# ==================== persist_results ====================


class TestPersistResults:
    @pytest.fixture
    def mock_db_session(self, mocker):
        """Mock SessionLocal context manager and return the session."""
        mock_session = mocker.MagicMock()
        mocker.patch(
            "stock_prediction_ml.model.batch_predict.SessionLocal",
            return_value=mock_session,
        )
        mock_session.__enter__ = mocker.MagicMock(return_value=mock_session)
        mock_session.__exit__ = mocker.MagicMock(return_value=False)
        return mock_session

    def _make_db_rows(self, mocker, symbols):
        """Create mock RawStockData rows for the given symbols."""
        rows = []
        for sym in symbols:
            row = mocker.MagicMock(spec=RawStockData)
            row.symbol = sym
            rows.append(row)
        return rows

    def test_returns_correct_success_fail_counts(self, mocker, mock_db_session, sample_result_df):
        rows = self._make_db_rows(mocker, ["AAPL", "MSFT"])
        mock_db_session.execute.return_value.scalars.return_value.all.return_value = rows

        ok, fail = persist_results(sample_result_df, ["AAPL", "MSFT"], "2024-01-01", "v1")
        assert ok == 2
        assert fail == 0

    def test_missing_raw_data_increments_fail_count(
        self, mocker, mock_db_session, sample_result_df
    ):
        rows = self._make_db_rows(mocker, ["AAPL", "TEST"])
        mock_db_session.execute.return_value.scalars.return_value.all.return_value = rows

        ok, fail = persist_results(sample_result_df, ["AAPL", "TEST"], "2024-01-01", "v1")
        assert ok == 1
        assert fail == 1

    def test_up_prediction_uses_proba_up(self, mocker, mock_db_session, sample_result_df):
        rows = self._make_db_rows(mocker, ["AAPL", "MSFT"])
        mock_db_session.execute.return_value.scalars.return_value.all.return_value = rows

        persist_results(sample_result_df, ["AAPL", "MSFT"], "2024-01-01", "v1")

        assert mock_db_session.add_all.call_args[0][0][0].probability == 0.72

    def test_down_prediction_uses_proba_down(self, mocker, mock_db_session, sample_result_df):
        rows = self._make_db_rows(mocker, ["AAPL", "MSFT"])
        mock_db_session.execute.return_value.scalars.return_value.all.return_value = rows

        persist_results(sample_result_df, ["AAPL", "MSFT"], "2024-01-01", "v1")

        assert mock_db_session.add_all.call_args[0][0][1].probability == 0.65

    def test_commit_is_called(self, mocker, mock_db_session, sample_result_df):
        rows = self._make_db_rows(mocker, ["AAPL", "MSFT"])
        mock_db_session.execute.return_value.scalars.return_value.all.return_value = rows

        persist_results(sample_result_df, ["AAPL", "MSFT"], "2024-01-01", "v1")

        mock_db_session.commit.assert_called_once()


# ==================== main (integration) ====================


class TestMain:
    def test_calls_all_steps_in_order(self, mocker):
        mock_load = mocker.patch("stock_prediction_ml.model.batch_predict.load_champion_model")
        mock_fetch = mocker.patch("stock_prediction_ml.model.batch_predict.fetch_features")
        mock_predict = mocker.patch("stock_prediction_ml.model.batch_predict.run_predictions")
        mock_persist = mocker.patch("stock_prediction_ml.model.batch_predict.persist_results")

        mock_load.return_value = (mocker.MagicMock(), "1")
        mock_persist.return_value = (2, 0)

        main(["AAPL", "MSFT"], "2024-01-01")

        mock_load.assert_called_once()
        mock_fetch.assert_called_once()
        mock_predict.assert_called_once()
        mock_persist.assert_called_once()

    def test_raises_on_model_load_failure(self, mocker):
        mock_load = mocker.patch(
            "stock_prediction_ml.model.batch_predict.load_champion_model",
            side_effect=Exception("Failed model load"),
        )
        mock_fetch = mocker.patch("stock_prediction_ml.model.batch_predict.fetch_features")

        with pytest.raises(Exception, match="Failed model load"):
            main(["AAPL", "MSFT"], "2024-01-01")

        mock_load.assert_called_once()
        mock_fetch.assert_not_called()

    def test_raises_on_feature_fetch_failure(self, mocker):
        mock_load = mocker.patch("stock_prediction_ml.model.batch_predict.load_champion_model")
        mock_fetch = mocker.patch(
            "stock_prediction_ml.model.batch_predict.fetch_features",
            side_effect=Exception("Failed fetching"),
        )
        mock_predict = mocker.patch("stock_prediction_ml.model.batch_predict.run_predictions")
        mock_persist = mocker.patch("stock_prediction_ml.model.batch_predict.persist_results")

        mock_load.return_value = (mocker.MagicMock(), "1")

        with pytest.raises(Exception, match="Failed fetching"):
            main(["AAPL", "MSFT"], "2024-01-01")

        mock_load.assert_called_once()
        mock_fetch.assert_called_once()
        mock_predict.assert_not_called()
        mock_persist.assert_not_called()
