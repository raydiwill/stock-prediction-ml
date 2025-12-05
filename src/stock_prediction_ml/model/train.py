import argparse
import json
import logging
from pathlib import Path

import joblib
import matplotlib.pyplot as plt
import mlflow
import pandas as pd
import yaml
from catboost import CatBoostClassifier
from mlflow.models import infer_signature
from sklearn.metrics import accuracy_score, roc_auc_score
from sklearn.preprocessing import OneHotEncoder

from src.stock_prediction_ml.config.settings import settings

# Configure enhanced logging with timestamps and better formatting
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


PROJECT_ROOT = Path(__file__).resolve().parents[3]


def log_section(title: str):
    """Log a formatted section header for better readability."""
    logger.info("=" * 80)
    logger.info(f"  {title.upper()}")
    logger.info("=" * 80)


def log_dict(title: str, data: dict, indent: int = 2):
    """Log a dictionary with nice formatting."""
    logger.info(f"{title}:")
    for key, value in data.items():
        logger.info(f"  {key}: {value}")


def log_dataframe_info(df_name: str, df):
    """Log DataFrame shape, columns, and date range."""
    logger.info(f"{df_name}:")
    logger.info(f"  Shape: {df.shape[0]} rows × {df.shape[1]} columns")
    logger.info(f"  Columns: {list(df.columns)[:5]}...")  # Show first 5 columns
    if "date" in df.columns:
        logger.info(f"  Date range: {df['date'].min()} to {df['date'].max()}")


def load_config(config_path: str | Path | None = None) -> dict:
    """Load training configuration from YAML file.

    Args:
        config_path (str | Path | None): Path to the configuration file.
                                        If None, defaults to local.yaml.

    Returns:
        dict: Configuration parameters.

    Example:
        >>> config = load_config("config/train/local.yaml")
        >>> print(config["training_data_path"])
        >>> data/feature/stock_eod_features.parquet
    """
    if config_path is None:
        config_path = PROJECT_ROOT / "config" / "train" / "local.yaml"
    else:
        config_path = Path(config_path)

    logger.info(f"Loading config from: {config_path}")
    with open(config_path) as f:
        config = yaml.safe_load(f)

    logger.info(f"Config loaded successfully with {len(config)} keys")
    return config


def load_selected_features(
    selected_features_path: str | Path | None = None,
) -> list[str]:
    """Load selected feature names from a JSON file.

    Args:
        selected_features_path (str | Path | None): Path to the JSON file.
            If None, defaults to data/meta/selected_features.json.

    Returns:
        list[str]: List of selected feature names.

    Example:
        >>> feats = load_selected_features("data/meta/selected_features.json")
        >>> feats[:3]
        ['return', 'volatility_5d', 'sma_10']
    """
    if selected_features_path is None:
        selected_features_path = (
            PROJECT_ROOT / "data" / "meta" / "selected_features.json"
        )
    else:
        selected_features_path = Path(selected_features_path)

    with open(selected_features_path) as f:
        selected_features_dict = json.load(f)

    selected_features = selected_features_dict["features"]
    return selected_features


def load_raw_training_data(feature_data_path: str | Path | None = None) -> pd.DataFrame:
    """Load raw feature data from parquet and normalize/sort for temporal splits.

    Args:
        feature_data_path (str | Path | None): Path to the parquet dataset.
            If None, defaults to data/feature/stock_eod_features.parquet.

    Returns:
        pd.DataFrame: DataFrame with 'date' as datetime and sorted by ['symbol', 'date'].

    Example:
        >>> df = load_raw_training_data("data/feature/stock_eod_features.parquet")
        >>> df[['symbol', 'date']].head()
    """
    if feature_data_path is None:
        feature_data_path = (
            PROJECT_ROOT / "data" / "feature" / "stock_eod_features.parquet"
        )
    else:
        feature_data_path = Path(feature_data_path)

    logger.info(f"Loading raw training data from: {feature_data_path}")
    df = pd.read_parquet(feature_data_path)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(by=["symbol", "date"])

    log_dataframe_info("Raw training data loaded", df)
    logger.info(f"Unique symbols: {df['symbol'].nunique()}")
    return df


def split_data_train_test(
    df: pd.DataFrame, test_size: float = 0.1
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Time-based train/test split using a date quantile cutoff.

    Args:
        df (pd.DataFrame): Input dataset with a 'date' column.
        test_size (float): Proportion of data to allocate to test (by date quantile).

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: (train_df, test_df) with no date overlap.

    Example:
        >>> train_df, test_df = split_data_train_test(df, test_size=0.2)
        >>> train_df['date'].max() < test_df['date'].min()
        True
    """
    cutoff = df["date"].quantile(1 - test_size)
    train = df[df["date"] < cutoff].copy()
    test = df[df["date"] >= cutoff].copy()

    logger.info(f"Split data with test_size={test_size:.1%}")
    logger.info(f"  Train: {len(train)} rows ({len(train)/len(df)*100:.1f}%)")
    logger.info(f"  Test:  {len(test)} rows ({len(test)/len(df)*100:.1f}%)")
    logger.info(f"  Cutoff date: {cutoff}")
    return train, test


def fit_and_save_encoder(
    train_df: pd.DataFrame,
    categorical_column: str = "symbol",
    meta_dir: str | Path | None = None,
):
    """Fit and persist a OneHotEncoder using the training subset only.

    Args:
        train_df (pd.DataFrame): Training DataFrame containing the categorical column.
        categorical_column (str): Column name to one-hot encode (default 'symbol').
        meta_dir (str | Path | None): Directory to store 'ohe.pkl'.
            If None, defaults to data/meta.

    Returns:
        Path: Path to the saved encoder (.pkl).

    Example:
        >>> path = fit_and_save_encoder(train_df, categorical_column="symbol", meta_dir="data/meta")
        >>> path.name
        'ohe.pkl'
    """
    if meta_dir is None:
        meta_dir = PROJECT_ROOT / "data" / "meta"
    else:
        meta_dir = Path(meta_dir)
    meta_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Fitting OneHotEncoder on '{categorical_column}' column")
    encoder = OneHotEncoder(sparse_output=False, handle_unknown="ignore")
    encoder.fit(train_df[[categorical_column]])

    n_categories = len(encoder.categories_[0])
    logger.info(f"  Categories found: {n_categories}")
    logger.info(f"  Categories: {list(encoder.categories_[0])}")

    encoder_path = meta_dir / "ohe.pkl"
    joblib.dump(encoder, encoder_path)
    logger.info(f"Saved OneHotEncoder to {encoder_path}")


def load_and_transform_with_encoder(
    df: pd.DataFrame,
    categorical_column: str = "symbol",
    meta_dir: str | Path | None = None,
    encoder_name: str | None = None,
):
    """Apply a persisted OneHotEncoder to a DataFrame and append encoded columns.

    Args:
        df (pd.DataFrame): Input DataFrame containing the categorical column.
        categorical_column (str): Column to encode (default 'symbol').
        meta_dir (str | Path | None): Directory containing encoder .pkl; defaults to data/meta.
        encoder_name (str | None): Custom encoder file name (with/without .pkl).

    Returns:
        pd.DataFrame: DataFrame with original column dropped and OHE columns added (sorted columns).

    Example:
        >>> df_enc = load_and_transform_with_encoder(df, meta_dir="data/meta")
        >>> [c for c in df_enc.columns if c.startswith('symbol_')][:3]
    """
    if meta_dir is None:
        meta_dir = PROJECT_ROOT / "data" / "meta"
    meta_dir = Path(meta_dir)

    if encoder_name is None:
        encoder_path = meta_dir / "ohe.pkl"
    else:
        encoder_path = meta_dir / (
            encoder_name if encoder_name.endswith(".pkl") else encoder_name + ".pkl"
        )

    encoder = joblib.load(encoder_path)
    matrix_encoded = encoder.transform(df[[categorical_column]])
    symbol_columns = encoder.get_feature_names_out([categorical_column])
    df_encoded = pd.DataFrame(matrix_encoded, columns=symbol_columns, index=df.index)

    if categorical_column in df.columns:
        df = df.drop(categorical_column, axis=1)

    final_df = pd.concat([df, df_encoded], axis=1)
    final_df = final_df.reindex(sorted(final_df.columns), axis=1)
    return final_df


def build_X_y(df, selected_features, target_column):
    """Create model input (X) and target (y) arrays from a DataFrame.

    Args:
        df (pd.DataFrame): Feature DataFrame (already encoded as needed).
        selected_features (list[str]): Columns to include in X.
        target_column (str): Target column name for y.

    Returns:
        tuple[np.ndarray, np.ndarray]: (X, y) arrays.

    Example:
        >>> X, y = build_X_y(df, selected_features, target_column="target")
        >>> X.shape, y.shape
    """
    X = df[selected_features].values
    y = df[target_column].values
    return X, y


def train_model(
    X_train, y_train, X_val, y_val, params, early_stopping_rounds=50, verbose=False
):
    """Train a CatBoostClassifier with early stopping and return run info.

    Args:
        X_train (np.ndarray): Training features.
        y_train (np.ndarray): Training labels.
        X_val (np.ndarray): Validation features.
        y_val (np.ndarray): Validation labels.
        params (dict): CatBoost hyperparameters (e.g., iterations, depth).
        early_stopping_rounds (int): Early stopping patience.
        verbose (bool): CatBoost fit verbosity.

    Returns:
        tuple[CatBoostClassifier, dict]: (model, info) with best_iteration, train_size, val_size.

    Example:
        >>> model, info = train_model(X_train, y_train, X_val, y_val, params={'iterations': 200})
        >>> info['best_iteration']
    """
    params = dict(params)
    params.setdefault("random_seed", 42)

    logger.info("Training CatBoost model...")
    logger.info(f"  Training samples: {len(y_train)}")
    logger.info(f"  Validation samples: {len(y_val)}")
    logger.info(f"  Features: {X_train.shape[1]}")
    log_dict("  Model params", params)

    model = CatBoostClassifier(**params)
    model.fit(
        X_train,
        y_train,
        eval_set=(X_val, y_val),
        early_stopping_rounds=early_stopping_rounds,
        verbose=verbose,
        use_best_model=True,
    )

    best_iter = (
        model.get_best_iteration() if hasattr(model, "get_best_iteration") else None
    )
    logger.info(f"Training complete. Best iteration: {best_iter}")

    info = {
        "best_iteration": int(best_iter) if best_iter is not None else None,
        "train_size": int(len(y_train)),
        "val_size": int(len(y_val)),
    }
    return model, info


def evaluate_model(model, X_test, y_test, prefix: str = "test") -> dict[str, float]:
    """Compute accuracy and ROC-AUC; keys prefixed for context (e.g., 'test_', 'val_').

    Args:
        model (CatBoostClassifier): Trained CatBoost model.
        X_test (np.ndarray): Features for evaluation.
        y_test (np.ndarray): Labels for evaluation.
        prefix (str): Metric key prefix ('test' or 'val').

    Returns:
        dict[str, float]: {f'{prefix}_accuracy': ..., f'{prefix}_roc_auc': ...}

    Example:
        >>> metrics = evaluate_model(model, X_test, y_test, prefix="test")
        >>> metrics['test_accuracy']
    """
    metrics = {}
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    accuracy = accuracy_score(y_test, y_pred)
    roc_auc = roc_auc_score(y_test, y_proba)

    metrics[f"{prefix}_accuracy"] = float(accuracy)
    metrics[f"{prefix}_roc_auc"] = float(roc_auc)

    logger.info(f"{prefix.capitalize()} metrics:")
    logger.info(f"  Accuracy: {accuracy:.4f}")
    logger.info(f"  ROC-AUC:  {roc_auc:.4f}")
    return metrics


def save_model(
    model, model_dir: str | Path | None = None, model_name: str = "catboost_model"
) -> Path:
    """Save CatBoost model to native .cbm format and return the file path.

    Args:
        model (CatBoostClassifier): Trained model to persist.
        model_dir (str | Path | None): Output directory; defaults to models/.
        model_name (str): Base filename without extension.

    Returns:
        Path: Path to the saved .cbm file.

    Example:
        >>> path = save_model(model, model_dir="models", model_name="stock_clf")
        >>> path.suffix
        '.cbm'
    """
    if model_dir is None:
        model_dir = PROJECT_ROOT / "models"
    model_dir = Path(model_dir)
    model_dir.mkdir(parents=True, exist_ok=True)

    model_path = model_dir / f"{model_name}.cbm"
    model.save_model(str(model_path))
    logger.info(
        f"Saved model to {model_path} ({model_path.stat().st_size/1024:.1f} KB)"
    )
    return model_path


def plot_feature_importance(model, feature_names, top_n=20, save_path=None):
    """Plot and save feature importance from trained CatBoost model.

    Args:
        model (CatBoostClassifier): Trained model.
        feature_names (list[str]): List of feature names.
        top_n (int): Number of top features to display.
        save_path (str | Path | None): Path to save the plot.

    Returns:
        Path: Path to saved plot file.

    Example:
        >>> plot_path = plot_feature_importance(model, selected_features, top_n=20)
    """
    # Get feature importances
    importances = model.get_feature_importance()

    # Create DataFrame and sort
    importance_df = (
        pd.DataFrame({"feature": feature_names, "importance": importances})
        .sort_values("importance", ascending=False)
        .head(top_n)
    )

    # Plot
    plt.figure(figsize=(10, 8))
    plt.barh(range(len(importance_df)), importance_df["importance"])
    plt.yticks(range(len(importance_df)), importance_df["feature"])
    plt.xlabel("Feature Importance")
    plt.title(f"Top {top_n} Feature Importances")
    plt.gca().invert_yaxis()
    plt.tight_layout()

    if save_path is None:
        save_path = Path("docs/images/feature_importance.png")
    else:
        save_path = Path(save_path)

    plt.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close()

    logger.info(f"Saved feature importance plot to {save_path}")
    return save_path


def plot_confusion_matrix(y_true, y_pred, save_path=None):
    """Plot confusion matrix for binary classification.

    Args:
        y_true (np.ndarray): True labels.
        y_pred (np.ndarray): Predicted labels.
        save_path (str | Path | None): Path to save plot.

    Returns:
        Path: Path to saved plot file.
    """
    from sklearn.metrics import ConfusionMatrixDisplay, confusion_matrix

    cm = confusion_matrix(y_true, y_pred)

    _, ax = plt.subplots(figsize=(8, 6))
    disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=["Down", "Up"])
    disp.plot(ax=ax, cmap="Blues", values_format="d")
    plt.title("Confusion Matrix")
    plt.tight_layout()

    if save_path is None:
        save_path = Path("docs/images/confusion_matrix.png")
    else:
        save_path = Path(save_path)

    plt.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close()

    logger.info(f"Saved confusion matrix to {save_path}")
    return save_path


def plot_roc_curve(y_true, y_proba, save_path=None):
    """Plot ROC curve.

    Args:
        y_true (np.ndarray): True labels.
        y_proba (np.ndarray): Predicted probabilities for positive class.
        save_path (str | Path | None): Path to save plot.

    Returns:
        Path: Path to saved plot file.
    """
    from sklearn.metrics import auc, roc_curve

    fpr, tpr, _ = roc_curve(y_true, y_proba)
    roc_auc = auc(fpr, tpr)

    plt.figure(figsize=(8, 6))
    plt.plot(
        fpr, tpr, color="darkorange", lw=2, label=f"ROC curve (AUC = {roc_auc:.3f})"
    )
    plt.plot([0, 1], [0, 1], color="navy", lw=2, linestyle="--", label="Random")
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel("False Positive Rate")
    plt.ylabel("True Positive Rate")
    plt.title("Receiver Operating Characteristic (ROC) Curve")
    plt.legend(loc="lower right")
    plt.grid(alpha=0.3)
    plt.tight_layout()

    if save_path is None:
        save_path = Path("docs/images/roc_curve.png")
    else:
        save_path = Path(save_path)

    plt.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close()

    logger.info(f"Saved ROC curve to {save_path}")
    return save_path


def main(config_path: str | Path | None = None):
    """End-to-end training pipeline with MLflow logging.

    Steps:
        1) Load config, data, and selected features
        2) Temporal split into train/val/test (by quantiles)
        3) Fit encoder on train; apply to all splits
        4) Build X/y, train model with early stopping
        5) Evaluate (val/test), log metrics and artifacts
        6) Generate and log diagnostic plots
        7) Save model (.cbm) and log to MLflow

    Args:
        config_path (str | Path | None): Path to YAML config; defaults to local.yaml.

    Returns:
        None

    Example:
        >>> main("configs/training/local.yaml")
    """
    log_section("Stock Prediction Model Training")

    config = load_config(config_path)
    log_section("Loading data")
    df = load_raw_training_data(config.get("training_data_path"))
    selected_features = load_selected_features(config.get("selected_features_path"))
    logger.info(f"Selected features: {len(selected_features)} features")

    log_section("Starting MLflow experiment")
    # Extract MLflow settings from nested dict; fall back to defaults
    tracking_uri = settings.mlflow_tracking_uri
    mlflow_config = config.get("mlflow", {})
    experiment_name = mlflow_config.get("experiment_name", "stock_prediction")
    run_name = mlflow_config.get("run_name", None)

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment_name)

    with mlflow.start_run(run_name=run_name):
        logger.info(f"MLflow run ID: {mlflow.active_run().info.run_id}")
        logger.info(f"MLflow tracking URI: {tracking_uri}")
        logger.info(f"MLflow experiment: {experiment_name}")

        mlflow.log_artifact(
            config_path or "configs/training/local.yaml", artifact_path="config"
        )
        mlflow.log_params(config.get("model_params", {}))
        mlflow.log_param("selected_feature_count", len(selected_features))

        log_section("Splitting data")
        train_val_df, test_df = split_data_train_test(
            df, test_size=config.get("test_size", 0.1)
        )
        train_df, val_df = split_data_train_test(train_val_df, test_size=0.5)

        log_section("Encoding categorical features")
        fit_and_save_encoder(train_df, meta_dir=config.get("meta_dir"))
        mlflow.log_artifact(
            Path(config.get("meta_dir")) / "ohe.pkl", artifact_path="meta"
        )
        mlflow.log_artifact(
            Path(config.get("meta_dir")) / "selected_features.json",
            artifact_path="meta",
        )

        # Apply the same encoder to all splits for consistent feature space.
        train_df = load_and_transform_with_encoder(
            train_df, meta_dir=config.get("meta_dir")
        )
        val_df = load_and_transform_with_encoder(
            val_df, meta_dir=config.get("meta_dir")
        )
        test_df = load_and_transform_with_encoder(
            test_df, meta_dir=config.get("meta_dir")
        )

        X_train, y_train = build_X_y(train_df, selected_features, config["target"])
        X_val, y_val = build_X_y(val_df, selected_features, config["target"])
        X_test, y_test = build_X_y(test_df, selected_features, config["target"])

        log_section("Training model")
        model, info = train_model(
            X_train,
            y_train,
            X_val,
            y_val,
            params=config.get("model_params", {}),
            early_stopping_rounds=config.get("early_stopping_rounds", 50),
            verbose=config.get("verbose", False),
        )
        mlflow.log_params(info)

        log_section("Evaluating model")
        mlflow.log_metrics(evaluate_model(model, X_val, y_val, prefix="val"))
        mlflow.log_metrics(evaluate_model(model, X_test, y_test, prefix="test"))

        log_section("Generating diagnostics")
        y_test_pred = model.predict(X_test)
        y_test_proba = model.predict_proba(X_test)[:, 1]

        fi_plot = plot_feature_importance(model, selected_features, top_n=20)
        logger.info("Logged feature importance plot")
        mlflow.log_artifact(str(fi_plot), artifact_path="diagnostics")

        cm_plot = plot_confusion_matrix(y_test, y_test_pred)
        roc_plot = plot_roc_curve(y_test, y_test_proba)
        logger.info("Logged confusion matrix and ROC curve")
        mlflow.log_artifact(str(cm_plot), artifact_path="diagnostics")
        mlflow.log_artifact(str(roc_plot), artifact_path="diagnostics")

        log_section("Saving model")
        model_path = save_model(model, model_dir=config.get("model_dir"))
        mlflow.log_artifact(str(model_path), artifact_path="model")

        signature = infer_signature(X_train, model.predict(X_train))
        mlflow.catboost.log_model(
            model, 
            name="catboost_model", 
            signature=signature, 
            input_example=X_train[:5]
        )

        log_section("Training complete")
        logger.info(f"View results: mlflow ui --backend-store-uri {tracking_uri}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Train a CatBoost model for stock prediction."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to the training configuration YAML file.",
    )
    args = parser.parse_args()
    main(args.config)
