from unittest.mock import Mock

import pytest
import yaml

from stock_prediction_ml.model.promote import (
    evaluate_promotion,
    get_latest_version_from_experiment,
    get_run_metrics,
    load_promotion_config,
    promote,
)

# ==================== FIXTURES ====================


@pytest.fixture
def thresholds():
    """Default promotion thresholds matching DEFAULT_THRESHOLDS."""
    return {
        "champion": {"accuracy": 0.65, "auc": 0.70},
        "challenger": {"accuracy": 0.60, "auc": 0.65},
    }


@pytest.fixture
def config_path(tmp_path):
    """YAML config WITH promotion thresholds."""
    cfg = {
        "promotion": {
            "champion": {"accuracy": 0.70, "auc": 0.75},
            "challenger": {"accuracy": 0.55, "auc": 0.60},
        }
    }
    path = tmp_path / "config.yaml"
    path.write_text(yaml.dump(cfg))
    return path


@pytest.fixture
def config_path_no_promotion(tmp_path):
    """YAML config WITHOUT promotion key."""
    cfg = {"training_data_path": "data/features.parquet"}
    path = tmp_path / "config_no_promo.yaml"
    path.write_text(yaml.dump(cfg))
    return path


@pytest.fixture
def mock_client():
    """MlflowClient mock with default return values wired for all public functions."""
    client = Mock()
    mock_run = Mock()
    mock_version = Mock()

    client.get_experiment_by_name.return_value.experiment_id = "abc123"

    mock_run.info.run_id = "def123"
    client.search_runs.return_value = [mock_run]

    client.get_run.return_value.data.metrics = {
        "test_accuracy": 0.65,
        "test_roc_auc": 0.55,
    }

    mock_version.version = 3
    client.search_model_versions.return_value = [mock_version]

    client.get_model_version.return_value.run_id = "ghk123"

    return client


# ==================== load_promotion_config ====================


class TestLoadPromotionConfig:
    def test_loads_custom_thresholds(self, config_path):
        """Should load champion/challenger thresholds from YAML."""
        promotion_dict = load_promotion_config(config_path)

        assert promotion_dict["champion"] == {"accuracy": 0.70, "auc": 0.75}
        assert promotion_dict["challenger"] == {"accuracy": 0.55, "auc": 0.60}

    def test_raises_on_missing_file(self):
        """Should raise FileNotFoundError for a non-existent config path."""
        with pytest.raises(FileNotFoundError):
            load_promotion_config("/nonexistent/path.yaml")

    def test_falls_back_to_defaults(self, config_path_no_promotion):
        """Should return DEFAULT_THRESHOLDS when YAML lacks 'promotion' key."""

        promotion_dict = load_promotion_config(config_path_no_promotion)
        champion = promotion_dict.get("champion")
        challenger = promotion_dict.get("challenger")

        assert champion["accuracy"] == 0.65
        assert challenger["auc"] == 0.65

    def test_has_both_tiers(self, config_path):
        """Result should always have both 'champion' and 'challenger' keys."""
        promotion_dict = load_promotion_config(config_path)

        assert set(promotion_dict.keys()) == {"champion", "challenger"}


# ==================== evaluate_promotion ====================


class TestEvaluatePromotion:
    def test_returns_champion(self, thresholds):
        """Metrics above champion thresholds → 'champion'."""
        sample_metrics = {"test_accuracy": 0.66, "test_roc_auc": 0.71}

        alias = evaluate_promotion(metrics=sample_metrics, thresholds=thresholds)

        assert alias == "champion"

    def test_returns_challenger(self, thresholds):
        """Metrics above challenger but below champion → 'challenger'."""
        sample_metrics = {"test_accuracy": 0.66, "test_roc_auc": 0.65}

        alias = evaluate_promotion(metrics=sample_metrics, thresholds=thresholds)

        assert alias == "challenger"

    def test_returns_none_below_all(self, thresholds):
        """Metrics below all thresholds → None."""
        sample_metrics = {"test_accuracy": 0.40, "test_roc_auc": 0.30}

        alias = evaluate_promotion(metrics=sample_metrics, thresholds=thresholds)

        assert alias is None

    def test_handles_missing_keys(self, thresholds):
        """Missing metric keys default to 0 via .get() → should return None."""
        sample_metrics = {}

        alias = evaluate_promotion(metrics=sample_metrics, thresholds=thresholds)

        assert alias is None

    def test_champion_at_exact_threshold(self, thresholds):
        """Metrics exactly at champion thresholds → 'champion' (>= is inclusive)."""
        sample_metrics = {
            "test_accuracy": 0.65,
            "test_roc_auc": 0.70,
        }

        alias = evaluate_promotion(metrics=sample_metrics, thresholds=thresholds)

        assert alias == "champion"


# ==================== get_latest_version_from_experiment ====================


class TestGetLatestVersionFromExperiment:
    def test_returns_version_number(self, mock_client):
        """Should return int version from latest run."""
        version = get_latest_version_from_experiment(mock_client, "abc123", "abc123")

        assert version == 3

    def test_raises_when_no_experiment(self, mock_client):
        """ValueError if experiment doesn't exist."""
        mock_client.get_experiment_by_name.side_effect = ValueError("Value Error!")

        with pytest.raises(ValueError):
            get_latest_version_from_experiment(mock_client, "abcd123", "abc123")

    def test_raises_when_no_runs(self, mock_client):
        """ValueError if experiment has no runs."""

        mock_client.search_runs.return_value = []

        with pytest.raises(ValueError):
            get_latest_version_from_experiment(mock_client, "abc123", "abc123")

    def test_raises_when_no_model_versions(self, mock_client):
        """ValueError if run has no registered model versions."""

        mock_client.search_model_versions.return_value = []

        with pytest.raises(ValueError):
            get_latest_version_from_experiment(mock_client, "abc123", "abc123")


# ==================== get_run_metrics ====================


class TestGetRunMetrics:
    def test_returns_expected_keys(self, mock_client):
        """Should return dict with test_accuracy and test_roc_auc."""
        metrics_dict = get_run_metrics(mock_client, "abd123", 3)

        for key in metrics_dict.keys():
            assert key in ["test_accuracy", "test_roc_auc"]

    def test_values_are_floats(self, mock_client):
        """Metric values should be non-None floats."""
        metrics_dict = get_run_metrics(mock_client, "abd123", 3)

        for value in metrics_dict.values():
            assert value is not None
            assert isinstance(value, float)


# ==================== promote ====================


class TestPromote:
    def test_sets_alias(self, mock_client):
        """Should call set_registered_model_alias with correct args."""
        promote(mock_client, "abc123", 3, "champion")

        mock_client.set_registered_model_alias.assert_called_once_with(
            name="abc123", alias="champion", version=3
        )

    def test_updates_description_with_metrics(self, mock_client):
        """Should include accuracy and AUC in version description."""
        sample_metrics = {"test_accuracy": 0.66, "test_roc_auc": 0.71}

        promote(mock_client, "abc123", 3, "champion", sample_metrics)

        call_args = mock_client.update_model_version.call_args.kwargs
        assert "0.66" in call_args["description"]
        assert "0.71" in call_args["description"]

    def test_empty_metrics_sets_force_description(self, mock_client):
        """Empty metrics dict (falsy) → description should be 'Force-promoted'."""
        promote(mock_client, "abc123", 3, "champion", {})

        call_args = mock_client.update_model_version.call_args.kwargs
        assert "Force-promoted" in call_args["description"]

    def test_none_metrics_sets_force_description(self, mock_client):
        """metrics=None → description should be 'Force-promoted'."""
        promote(mock_client, "abc123", 3, "champion", None)

        call_args = mock_client.update_model_version.call_args.kwargs
        assert "Force-promoted" in call_args["description"]
