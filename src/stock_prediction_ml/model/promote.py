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
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Default thresholds (overridden by config)
DEFAULT_THRESHOLDS = {
    "champion": {"accuracy": 0.65, "auc": 0.70},
    "challenger": {"accuracy": 0.60, "auc": 0.65},
}
PROJECT_ROOT = Path(__file__).resolve().parents[3]


def load_promotion_config(config_path: str | Path | None) -> dict:
    """Load promotion thresholds from YAML config.

    Args:
        config_path: Path to YAML config file. If None or missing
            'promotion' key, returns DEFAULT_THRESHOLDS.

    Returns:
        Dict with 'champion' and 'challenger' threshold dicts,
        each containing 'accuracy' and 'auc' keys.

    Hint:
        - yaml.safe_load the file
        - Look for a 'promotion' key
        - Fall back to DEFAULT_THRESHOLDS if missing
    """
    if config_path is None:
        config_path = PROJECT_ROOT / "config" / "train" / "local.yaml"
    else:
        config_path = Path(config_path)

    logger.info(f"Loading config from: {config_path}")
    with open(config_path) as f:
        config = yaml.safe_load(f)

    logger.info(f"Config loaded successfully with {len(config)} keys")

    promote_dict = {}
    promote_dict["champion"] = config.get("champion")
    promote_dict["challenger"] = config.get("challenger")

    if promote_dict:
        return promote_dict
    else:
        return DEFAULT_THRESHOLDS
    


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
        The version number (int) of the model registered in the latest run.

    Raises:
        ValueError: If no runs found or no registered model version in the latest run.

    Hints:
        - Use mlflow.search_runs() with experiment_names=[experiment_name]
            and order_by=["start_time DESC"], max_results=1
        - The run's artifact contains the registered model version;
            alternatively, use client.search_model_versions() filtered
            by run_id to find the version
    """
    # TODO: implement
    pass


def get_run_metrics(
    client: MlflowClient,
    model_name: str,
    version: int,
) -> dict[str, float]:
    """Fetch training metrics from the MLflow run that produced this model version.

    Args:
        client: MLflow tracking client.
        model_name: Registered model name.
        version: Model version number.

    Returns:
        Dict with metric keys (e.g. 'test_accuracy', 'test_roc_auc')
        and their float values.

    Hints:
        - client.get_model_version() gives you the ModelVersion object
        - ModelVersion.run_id links back to the training run
        - client.get_run(run_id).data.metrics has all logged metrics
    """
    # TODO: implement
    pass


def evaluate_promotion(
    metrics: dict[str, float],
    thresholds: dict,
) -> str | None:
    """Determine which alias (if any) a model version qualifies for.

    Args:
        metrics: Dict with 'test_accuracy' and 'test_roc_auc' keys.
        thresholds: Dict with 'champion' and 'challenger' sub-dicts,
            each containing 'accuracy' and 'auc' float thresholds.

    Returns:
        "champion", "challenger", or None if below all thresholds.

    Logic:
        - Check champion thresholds first (higher bar)
        - Then challenger
        - Return None if neither met
    """
    # TODO: implement
    pass


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

    Hints:
        - client.set_registered_model_alias(name, alias, version)
        - client.update_model_version(name, str(version), description=...)
        - Build a description string from metrics for traceability
    """
    # TODO: implement
    pass


def main() -> None:
    """CLI entry point for model promotion.

    Parses args, resolves version, evaluates thresholds, promotes.

    Flow:
        1. Parse CLI args (--config, --version, --force, --alias, --model-name)
        2. Set up MLflow tracking URI
        3. If --version not provided, find latest from experiment
        4. If --force, skip threshold check and promote directly
        5. Otherwise, fetch metrics → evaluate → promote if qualified
    """
    parser = argparse.ArgumentParser(
        description="Promote a registered MLflow model version to champion/challenger."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to YAML config with promotion thresholds (default: uses built-in thresholds)",
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

    # TODO: implement main flow
    # Hints:
    # 1. Set mlflow tracking URI from settings
    # 2. Create MlflowClient
    # 3. Resolve model_name from args or settings
    # 4. Resolve version: if not provided, call get_latest_version_from_experiment
    #    - Need experiment_name from config or settings.default_experiment_name
    # 5. If --force: validate --alias is provided, then call promote() directly
    # 6. If not --force:
    #    - Load thresholds via load_promotion_config(args.config)
    #    - Fetch metrics via get_run_metrics()
    #    - Evaluate via evaluate_promotion()
    #    - If alias determined, call promote()
    #    - If no alias, log warning and exit
    promote_dict = load_promotion_config(args.config)



if __name__ == "__main__":
    main()
