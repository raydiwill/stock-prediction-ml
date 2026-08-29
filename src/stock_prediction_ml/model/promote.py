"""Model Promotion Script.

Standalone script to promote registered MLflow model versions
to champion/challenger aliases based on performance thresholds.

Separates promotion concern from training, enabling:
- First-run promotion with --force flag
- Re-promotion of existing versions
- Orchestration integration (Airflow DAGs)

Usage:
    # Auto-promote latest version from last training run
    $ python -m stock_prediction_ml.model.promote --config configs/training/local.yaml

    # Promote specific version
    $ python -m stock_prediction_ml.model.promote --version 3 --config configs/training/local.yaml

    # Force-promote (first run / manual override)
    $ python -m stock_prediction_ml.model.promote --version 1 --force --alias champion
"""

import argparse
import logging
from pathlib import Path

import mlflow
import yaml
from mlflow.tracking import MlflowClient

from stock_prediction_ml.config.settings import settings

logger = logging.getLogger(__name__)

DEFAULT_THRESHOLDS = {
    "champion": {"accuracy": 0.65, "auc": 0.70},
    "challenger": {"accuracy": 0.60, "auc": 0.65},
}
PROJECT_ROOT = Path(__file__).resolve().parents[3]


def load_promotion_config(config_path: str | Path | None) -> dict:
    """Load promotion thresholds from YAML config.

    Args:
        config_path: Path to YAML config file. If None, falls back to
            the default local config. If the file lacks a 'promotion'
            key, returns DEFAULT_THRESHOLDS.

    Returns:
        Dict with 'champion' and 'challenger' threshold dicts,
        each containing 'accuracy' and 'auc' keys.
    """
    config_path = (
        Path(config_path)
        if config_path
        else PROJECT_ROOT / "configs" / "training" / "local.yaml"
    )

    logger.info("Loading config from: %s", config_path)
    with open(config_path) as f:
        config = yaml.safe_load(f)

    thresholds = config.get("promotion", {})

    if not thresholds.get("champion") and not thresholds.get("challenger"):
        logger.info("No promotion thresholds in config, using defaults")
        return DEFAULT_THRESHOLDS

    return {
        "champion": thresholds.get("champion", DEFAULT_THRESHOLDS["champion"]),
        "challenger": thresholds.get("challenger", DEFAULT_THRESHOLDS["challenger"]),
    }


def get_latest_version_from_experiment(
    client: MlflowClient,
    experiment_name: str,
    model_name: str,
) -> int:
    """Find the model version registered by the most recent training run.

    Args:
        client: MLflow tracking client.
        experiment_name: Name of the MLflow experiment to search.
        model_name: Registered model name to filter by.

    Returns:
        The version number of the model registered in the latest run.

    Raises:
        ValueError: If no runs found or no registered model version
            in the latest run.
    """
    experiment = client.get_experiment_by_name(experiment_name)
    if not experiment:
        raise ValueError(f"Experiment '{experiment_name}' not found")

    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        order_by=["start_time DESC"],
        max_results=1,
    )
    if not runs:
        raise ValueError(f"No runs found in experiment '{experiment_name}'")

    run_id = runs[0].info.run_id
    logger.info("Latest run: %s", run_id)

    versions = client.search_model_versions(
        filter_string=f"name = '{model_name}' AND run_id = '{run_id}'"
    )
    if not versions:
        raise ValueError(
            f"No registered version of '{model_name}' found for run {run_id}"
        )

    version = int(versions[0].version)
    logger.info("Resolved latest model version: %d", version)
    return version


def get_run_metrics(
    client: MlflowClient,
    model_name: str,
    version: int,
) -> dict[str, float]:
    """Fetch training metrics from the MLflow run that produced a model version.

    Args:
        client: MLflow tracking client.
        model_name: Registered model name.
        version: Model version number.

    Returns:
        Dict with 'test_accuracy' and 'test_roc_auc' values.
    """
    run_id = client.get_model_version(name=model_name, version=version).run_id
    all_metrics = client.get_run(run_id).data.metrics

    metrics = {
        "test_accuracy": all_metrics.get("test_accuracy"),
        "test_roc_auc": all_metrics.get("test_roc_auc"),
    }
    logger.info(
        "Version %d metrics — accuracy: %s, AUC: %s",
        version,
        metrics["test_accuracy"],
        metrics["test_roc_auc"],
    )
    return metrics


def evaluate_promotion(
    metrics: dict[str, float],
    thresholds: dict,
) -> str | None:
    """Determine which alias (if any) a model version qualifies for.

    Checks champion thresholds first (higher bar), then challenger.

    Args:
        metrics: Dict with 'test_accuracy' and 'test_roc_auc' keys.
        thresholds: Dict with 'champion' and 'challenger' sub-dicts,
            each containing 'accuracy' and 'auc' float thresholds.

    Returns:
        "champion", "challenger", or None if below all thresholds.
    """
    accuracy = metrics.get("test_accuracy", 0)
    auc = metrics.get("test_roc_auc", 0)

    for alias in ("champion", "challenger"):
        tier = thresholds[alias]
        if accuracy >= tier["accuracy"] and auc >= tier["auc"]:
            logger.info(
                "Model qualifies for '%s' (accuracy=%.4f >= %.2f, auc=%.4f >= %.2f)",
                alias,
                accuracy,
                tier["accuracy"],
                auc,
                tier["auc"],
            )
            return alias

    logger.warning(
        "Metrics below all thresholds (accuracy=%.4f, auc=%.4f) — no alias assigned",
        accuracy,
        auc,
    )
    return None


def promote(
    client: MlflowClient,
    model_name: str,
    version: int,
    alias: str,
    metrics: dict[str, float] | None = None,
) -> None:
    """Assign an alias to a model version and update its description.

    Args:
        client: MLflow tracking client.
        model_name: Registered model name.
        version: Model version to promote.
        alias: Alias to assign ("champion" or "challenger").
        metrics: Optional metrics dict for description. If None,
            description is set to "Force-promoted".
    """
    client.set_registered_model_alias(name=model_name, alias=alias, version=version)

    description = (
        f"Accuracy: {metrics['test_accuracy']}; AUC ROC: {metrics['test_roc_auc']}"
        if metrics
        else "Force-promoted"
    )
    client.update_model_version(
        name=model_name, version=str(version), description=description
    )

    logger.info(
        "Promoted '%s' v%d → alias '%s' (%s)", model_name, version, alias, description
    )


def main() -> None:
    """CLI entry point for model promotion."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(handler)

    parser = argparse.ArgumentParser(
        description="Promote a registered MLflow model version to champion/challenger."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to YAML config with promotion thresholds (default: built-in thresholds)",
    )
    parser.add_argument(
        "--version",
        type=int,
        default=None,
        help="Model version to promote (default: latest from last training run)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force promotion, bypassing threshold checks (useful for first run)",
    )
    parser.add_argument(
        "--alias",
        type=str,
        default=None,
        help="Target alias (default: auto-detect from thresholds). Required with --force.",
    )
    parser.add_argument(
        "--model-name",
        type=str,
        default=None,
        help=f"Registered model name (default: '{settings.registered_model_name}')",
    )
    args = parser.parse_args()

    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    client = MlflowClient()
    model_name = args.model_name or settings.registered_model_name

    # Resolve experiment name: config file > settings default
    experiment_name = settings.default_experiment_name
    if args.config:
        config_path = Path(args.config)
        with open(config_path) as f:
            raw_config = yaml.safe_load(f)
        experiment_name = raw_config.get("mlflow", {}).get(
            "experiment_name", experiment_name
        )
        model_name = args.model_name or raw_config.get("mlflow", {}).get(
            "registered_model_name", model_name
        )

    # Resolve version: explicit arg or latest from experiment
    if args.version:
        version = args.version
        logger.info("Using explicit version: %d", version)
    else:
        logger.info("No version specified, resolving latest from experiment")
        version = get_latest_version_from_experiment(
            client,
            experiment_name,
            model_name,
        )

    if args.force:
        alias = args.alias or "champion"
        logger.info("Force-promoting version %d as '%s'", version, alias)
        promote(client, model_name, version, alias)
    else:
        thresholds = load_promotion_config(args.config)
        metrics = get_run_metrics(client, model_name, version)
        alias = evaluate_promotion(metrics, thresholds)
        if alias:
            promote(client, model_name, version, alias, metrics)
        else:
            logger.warning("No promotion applied for version %d", version)


if __name__ == "__main__":
    main()
