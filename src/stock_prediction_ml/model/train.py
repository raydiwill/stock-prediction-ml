"""Stock Prediction Model Training Pipeline.

This module provides an end-to-end ML training pipeline for predicting
next-day stock price direction (up/down) using CatBoost classifier.

Pipeline Steps:
    1. Load training data from Feast offline store
    2. Temporal train/val/test split (no data leakage)
    3. Fit OneHotEncoder on training data only
    4. Train CatBoost with early stopping
    5. Evaluate on validation and test sets
    6. Log metrics, artifacts, and model to MLflow
    7. Register model and assign alias based on performance

Key Components:
    - StockPredictionModel: MLflow pyfunc wrapper bundling model + encoder
    - promote_model_with_alias: Automated model promotion based on thresholds
    - save_model_artifacts_locally: Prepare artifacts for pyfunc logging

Usage:
    CLI:
        $ python -m stock_prediction_ml.model.train --config configs/training/local.yaml

    Python:
        >>> from stock_prediction_ml.model.train import main
        >>> main("configs/training/local.yaml")

Example Output:
    - MLflow experiment with logged metrics, params, and artifacts
    - Registered model with "champion" or "challenger" alias
    - Diagnostic plots (feature importance, confusion matrix, ROC curve)
"""

import argparse
import json
import logging
import shutil
from pathlib import Path

import joblib
import matplotlib.pyplot as plt
import mlflow
import mlflow.pyfunc
import pandas as pd
import yaml
from catboost import CatBoostClassifier
from feast import FeatureStore
from mlflow.models import infer_signature
from mlflow.tracking import MlflowClient
from sklearn.metrics import accuracy_score, roc_auc_score
from sklearn.preprocessing import OneHotEncoder

from stock_prediction_ml.config.settings import settings

# Configure enhanced logging with timestamps and better formatting
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


PROJECT_ROOT = Path(__file__).resolve().parents[3]


class StockPredictionModel(mlflow.pyfunc.PythonModel):
    """MLflow PyFunc wrapper that bundles model, encoder, and feature config.

    This class creates a self-contained model artifact for deployment.
    It handles symbol encoding internally, so callers only need to provide
    raw features + symbol column.

    Attributes:
        model (CatBoostClassifier): Trained classification model.
        encoder (OneHotEncoder): Fitted encoder for symbol column.
        selected_features (list[str]): Feature names used during training.

    Artifact Structure (saved by save_model_artifacts_locally):
        - model_path: catboost_model.cbm (CatBoost native format)
        - encoder_path: ohe.pkl (OneHotEncoder via joblib)
        - feature_path: selected_features.json (feature names)

    Example:
        Loading and using the model:
            >>> model = mlflow.pyfunc.load_model("models:/stock_prediction_classifier@champion")
            >>> input_df = pd.DataFrame({"symbol": ["AAPL"], "return": [0.01], ...})
            >>> predictions = model.predict(input_df)
            >>> predictions["prediction_class"].iloc[0]  # 0 or 1
    """

    def load_context(self, context) -> None:
        """Load model artifacts when the pyfunc model is loaded.

        Called automatically by MLflow when loading the model via
        mlflow.pyfunc.load_model(). Reads artifacts from the paths
        specified during log_model().

        Args:
            context: MLflow context object containing:
                - artifacts (dict): Mapping of artifact keys to local file paths
                    - "model_path": Path to catboost_model.cbm
                    - "encoder_path": Path to ohe.pkl
                    - "feature_path": Path to selected_features.json

        Sets:
            self.model: Loaded CatBoostClassifier
            self.encoder: Loaded OneHotEncoder
            self.selected_features: List of feature names
        """
        import json

        import joblib
        from catboost import CatBoostClassifier

        self.model = CatBoostClassifier()
        self.model.load_model(context.artifacts["model_path"])

        self.encoder = joblib.load(context.artifacts["encoder_path"])

        with open(context.artifacts["feature_path"]) as file:
            self.selected_features = json.load(file)["features"]

        logger.info("Loaded artifact for prediction!")

    def predict(self, context, model_input: pd.DataFrame) -> pd.DataFrame:
        """Generate predictions for input data.

        Handles symbol encoding internally using the bundled encoder,
        so input only needs raw features + symbol column.

        Args:
            context: MLflow context (unused but required by pyfunc interface).
            model_input: DataFrame containing:
                - "symbol" column (str): Stock ticker for encoding
                - Feature columns matching self.selected_features

        Returns:
            pd.DataFrame with columns:
                - prediction_class: Binary prediction (0=DOWN, 1=UP)
                - prediction_proba_up: Probability of UP class
                - prediction_proba_down: Probability of DOWN class

        Raises:
            ValueError: If "symbol" column is missing from input.

        Example:
            >>> input_df = pd.DataFrame({
            ...     "symbol": ["AAPL"],
            ...     "return": [0.01],
            ...     "volume": [1000000],
            ...     # ... other features
            ... })
            >>> result = model.predict(input_df)
            >>> result["prediction_class"].iloc[0]  # 0 or 1
        """
        # Validate input has required column
        if "symbol" not in model_input.columns:
            raise ValueError("Input DataFrame must contain 'symbol' column")

        features = [f for f in self.selected_features if not f.startswith("symbol_")]
        X_features = model_input[features]

        matrix_encoded = self.encoder.transform(model_input[["symbol"]])
        symbol_columns = self.encoder.get_feature_names_out(["symbol"])
        df_encoded = pd.DataFrame(matrix_encoded, columns=symbol_columns, index=model_input.index)

        final_df = pd.concat([X_features, df_encoded], axis=1)
        final_df = final_df.reindex(sorted(final_df.columns), axis=1)

        prediction_class = self.model.predict(final_df)
        prediction_proba = self.model.predict_proba(final_df)

        return pd.DataFrame(
            {
                "prediction_class": prediction_class.flatten(),
                "prediction_proba_up": prediction_proba[:, 1],
                "prediction_proba_down": prediction_proba[:, 0],
            }
        )


def log_section(title: str) -> None:
    """Log a formatted section header for pipeline readability.

    Creates a visual separator in logs to distinguish major pipeline steps.

    Args:
        title: Section title to display (will be uppercased).

    Example:
        >>> log_section("Training model")
        # Logs:
        # ================================================================================
        #   TRAINING MODEL
        # ================================================================================
    """
    logger.info("=" * 80)
    logger.info(f"  {title.upper()}")
    logger.info("=" * 80)


def log_dict(title: str, data: dict, indent: int = 2) -> None:
    """Log a dictionary with formatted key-value pairs.

    Args:
        title: Header text for the dictionary output.
        data: Dictionary to log.
        indent: Number of spaces for value indentation (default: 2).

    Example:
        >>> log_dict("Model params", {"depth": 6, "iterations": 500})
        # Logs:
        # Model params:
        #   depth: 6
        #   iterations: 500
    """
    logger.info(f"{title}:")
    for key, value in data.items():
        logger.info(f"  {key}: {value}")


def log_dataframe_info(df_name: str, df: pd.DataFrame) -> None:
    """Log DataFrame summary information.

    Displays shape, first 5 column names, and date range if applicable.

    Args:
        df_name: Descriptive name for the DataFrame.
        df: DataFrame to summarize.

    Example:
        >>> log_dataframe_info("Training data", train_df)
        # Logs:
        # Training data:
        #   Shape: 1500 rows × 25 columns
        #   Columns: ['symbol', 'date', 'open', 'high', 'low']...
        #   Date range: 2020-01-01 to 2025-12-31
    """
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
        selected_features_path = PROJECT_ROOT / "data" / "meta" / "selected_features.json"
    else:
        selected_features_path = Path(selected_features_path)

    with open(selected_features_path) as f:
        selected_features_dict = json.load(f)

    selected_features = selected_features_dict["features"]
    return selected_features


def load_training_data_from_feast(
    feature_service_name: str = "stock_training_service",
    feast_repo_path: str | Path | None = None,
) -> pd.DataFrame:
    """Load training features from Feast offline store using historical retrieval.

    Args:
        feature_service_name (str): Name of the Feast feature service to use.
        feast_repo_path (str | Path | None): Path to Feast repo;
                                            defaults to src/stock_prediction_ml/feast_repo.

    Returns:
        pd.DataFrame: Historical features with entity columns, timestamps, and all feature views.

    Example:
        >>> df = load_features_from_feast("stock_training_service")
        >>> df.columns
        Index(['symbol', 'date', 'open', 'high', ..., 'target'])
    """
    if feast_repo_path is None:
        feast_repo_path = PROJECT_ROOT / "src" / "stock_prediction_ml" / "feast_repo"
    else:
        feast_repo_path = Path(feast_repo_path)

    logger.info(f"Initializing FeatureStore from: {feast_repo_path}")
    store = FeatureStore(str(feast_repo_path))

    raw_feature_path = PROJECT_ROOT / "data" / "feature" / "stock_eod_features.parquet"
    raw_df = pd.read_parquet(raw_feature_path)

    # Prepare entity for point-in-time Feast
    entity_df = raw_df[["symbol", "date"]].copy()
    entity_df["date"] = pd.to_datetime(entity_df["date"])

    logger.info(f"Entity DataFrame shape: {entity_df.shape}")
    logger.info(f"Date range: {entity_df['date'].min()} to {entity_df['date'].max()}")

    # Retrieve historical features
    logger.info(f"Fetching features from service: {feature_service_name}")
    training_df = store.get_historical_features(
        entity_df=entity_df, features=store.get_feature_service(feature_service_name)
    ).to_df()

    # Sort to prepare for pipeline
    training_df = training_df.sort_values(["symbol", "date"])

    log_dataframe_info("Feast historical features loaded", training_df)
    logger.info(f"Unique symbols: {training_df['symbol'].nunique()}")

    return training_df


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
    logger.info(f"  Train: {len(train)} rows ({len(train) / len(df) * 100:.1f}%)")
    logger.info(f"  Test:  {len(test)} rows ({len(test) / len(df) * 100:.1f}%)")
    logger.info(f"  Cutoff date: {cutoff}")
    return train, test


def fit_encoder(
    train_df: pd.DataFrame,
    categorical_column: str = "symbol",
) -> OneHotEncoder:
    """Fit a OneHotEncoder using the training subset only.

    The encoder is returned in memory and will be bundled with the model
    via save_model_artifacts_locally() for MLflow pyfunc logging.

    Args:
        train_df (pd.DataFrame): Training DataFrame containing the categorical column.
        categorical_column (str): Column name to one-hot encode (default 'symbol').

    Returns:
        OneHotEncoder: The fitted encoder object.

    Example:
        >>> encoder = fit_encoder(train_df, categorical_column="symbol")
        >>> encoder.categories_[0]
        array(['AAPL', 'MSFT', 'TSLA'], dtype=object)
    """
    logger.info(f"Fitting OneHotEncoder on '{categorical_column}' column")
    encoder = OneHotEncoder(sparse_output=False, handle_unknown="ignore")
    encoder.fit(train_df[[categorical_column]])

    n_categories = len(encoder.categories_[0])
    logger.info(f"  Categories found: {n_categories}")
    logger.info(f"  Categories: {list(encoder.categories_[0])}")

    return encoder


def transform_with_encoder(
    df: pd.DataFrame,
    encoder: OneHotEncoder,
    categorical_column: str = "symbol",
) -> pd.DataFrame:
    """Apply a fitted OneHotEncoder to a DataFrame and append encoded columns.

    Args:
        df (pd.DataFrame): Input DataFrame containing the categorical column.
        encoder (OneHotEncoder): Fitted encoder object from fit_encoder().
        categorical_column (str): Column to encode (default 'symbol').

    Returns:
        pd.DataFrame: DataFrame with original column dropped and OHE columns added (sorted columns).

    Example:
        >>> encoder = fit_encoder(train_df)
        >>> df_enc = transform_with_encoder(df, encoder)
        >>> [c for c in df_enc.columns if c.startswith('symbol_')][:3]
        ['symbol_AAPL', 'symbol_MSFT', 'symbol_TSLA']
    """
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


def train_model(X_train, y_train, X_val, y_val, params, early_stopping_rounds=50, verbose=False):
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

    best_iter = model.get_best_iteration() if hasattr(model, "get_best_iteration") else None
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
        save_path = PROJECT_ROOT / "docs" / "images"

    # Add name
    save_path = save_path / "feature_importance.png"
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
        save_path = PROJECT_ROOT / "docs" / "images"

    # Add name
    save_path = save_path / "confusion_matrix.png"
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
    plt.plot(fpr, tpr, color="darkorange", lw=2, label=f"ROC curve (AUC = {roc_auc:.3f})")
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
        save_path = PROJECT_ROOT / "docs" / "images"

    # Add name
    save_path = save_path / "roc_curve.png"
    save_path = Path(save_path)

    plt.savefig(save_path, dpi=150, bbox_inches="tight")
    plt.close()

    logger.info(f"Saved ROC curve to {save_path}")
    return save_path


def clean_up_resources() -> None:
    """Clean up temporary resources after MLflow logging.

    Removes temporary files and directories created during training:
        - tmp/: Bundled model artifacts (catboost_model.cbm, ohe.pkl, etc.)
        - docs/images/*.png: Diagnostic plots (feature importance, ROC, etc.)
        - catboost_info/: CatBoost internal training logs

    Called at the end of main() to keep the workspace clean.
    Logs warnings but does not raise exceptions if cleanup fails.

    Example:
        >>> clean_up_resources()
        # Logs:
        # Cleaned up temporary artifacts in /path/to/tmp
        # Cleaned up temporary images in /path/to/docs/images
        # Cleaned up CatBoost folder!
    """
    # 1. Delete tmp/ directory with bundled artifacts
    tmp_dir = PROJECT_ROOT / "tmp"
    if tmp_dir.exists():
        try:
            shutil.rmtree(tmp_dir)
            logger.info(f"Cleaned up temporary artifacts in {tmp_dir}")
        except Exception as e:
            logger.warning(f"Failed to delete {tmp_dir}: {e}")

    # 2. Delete all generated images in docs/images
    images_dir = PROJECT_ROOT / "docs" / "images"
    if images_dir.exists():
        for img_file in images_dir.glob("*.png"):
            try:
                img_file.unlink()
            except Exception as e:
                logger.warning(f"Failed to delete {img_file}: {e}")
        logger.info(f"Cleaned up temporary images in {images_dir}")

    # 3. Clean up catboost local folder
    catboost_dir = PROJECT_ROOT / "catboost_info"
    if catboost_dir.exists():
        try:
            shutil.rmtree(catboost_dir)
            logger.info("Cleaned up CatBoost folder!")
        except Exception as e:
            logger.warning(f"Failed to delete folder {catboost_dir}: {e}")


def save_model_artifacts_locally(
    model: CatBoostClassifier,
    encoder: OneHotEncoder,
    selected_features: list[str],
    temp_dir: Path,
) -> dict[str, str]:
    """
    Save model artifacts to local temp files for bundling into pyfunc.

    This function prepares all artifacts needed by StockPredictionModel.load_context():
    - model.cbm (CatBoost native format)
    - encoder.pkl (OneHotEncoder via joblib)
    - selected_features.json (feature names)

    Args:
        model: Trained CatBoost model
        encoder: Fitted OneHotEncoder
        selected_features: List of feature names used for training
        temp_dir: Directory to save temporary artifacts

    Returns:
        dict: Mapping of artifact keys to file paths for mlflow.pyfunc.log_model()
            Keys must match what StockPredictionModel.load_context() expects:
            {
                "model_path": "/tmp/catboost_model.cbm",
                "encoder_path": "/tmp/ohe.pkl",
                "feature_path": "/tmp/selected_features.json",
            }

    Example:
        >>> artifact_paths = save_model_artifacts_locally(model, encoder, features, Path("/tmp"))
        >>> mlflow.pyfunc.log_model(
        ...     artifact_path="model",
        ...     python_model=StockPredictionModel(),
        ...     artifacts=artifact_paths,
        ... )
    """
    # Create temp_dir if it doesn't exist
    temp_dir.mkdir(parents=True, exist_ok=True)

    # Save CatBoost model in native format
    model_path = temp_dir / "catboost_model.cbm"
    model.save_model(fname=str(model_path))
    logger.info(f"Saved CatBoost model to {model_path}")

    # Save encoder with joblib
    encoder_path = temp_dir / "ohe.pkl"
    joblib.dump(encoder, encoder_path)
    logger.info(f"Saved OneHotEncoder to {encoder_path}")

    # Save selected_features as JSON (wrap in {"features": [...]})
    feature_path = temp_dir / "selected_features.json"
    with open(feature_path, "w") as file:
        json.dump({"features": selected_features}, file)
    logger.info(f"Saved selected features to {feature_path}")

    # Return dict with keys matching what load_context() expects
    return {
        "model_path": str(model_path),
        "encoder_path": str(encoder_path),
        "feature_path": str(feature_path),
    }


def promote_model_with_alias(
    model_name: str,
    version: int,
    test_metrics: dict[str, float],
) -> str | None:
    """
    Assign alias to a registered model version based on performance metrics.

    This function is called AFTER log_model() with registered_model_name parameter,
    which already creates the model version. This function only handles alias assignment.

    Args:
        model_name: Name of the registered model (e.g., "stock_prediction_classifier")
        version: Version number to potentially promote (from log_model return value)
        test_metrics: Evaluation metrics for promotion decision
            Expected keys: "test_accuracy", "test_roc_auc"

    Returns:
        str | None: The alias assigned ("champion", "challenger") or None if below thresholds

    Promotion thresholds:
        - "champion": AUC >= 0.60 AND Accuracy >= 0.55
        - "challenger": AUC >= 0.55 AND Accuracy >= 0.52
        - None: Below thresholds

    Loading models by alias:
        - Champion: mlflow.pyfunc.load_model("models:/{model_name}@champion")
        - Challenger: mlflow.pyfunc.load_model("models:/{model_name}@challenger")

    Example:
        >>> alias = promote_model_with_alias(
        ...     model_name="stock_prediction_classifier",
        ...     version=3,
        ...     test_metrics={"test_accuracy": 0.58, "test_roc_auc": 0.62},
        ... )
        >>> print(f"Assigned alias: {alias}")  # "champion"
    """
    client = MlflowClient()

    # Define promotion thresholds
    CHAMPION_THRESHOLD = {"auc": 0.70, "accuracy": 0.65}
    CHALLENGER_THRESHOLD = {"auc": 0.65, "accuracy": 0.60}

    # TODO: Step 1 - Extract metrics from test_metrics dict
    test_accuracy = test_metrics.get("test_accuracy", 0)
    test_auc_roc = test_metrics.get("test_roc_auc", 0)

    # TODO: Step 2 - Determine which alias (if any) based on thresholds
    alias = None
    meets_champion = (
        test_accuracy >= CHAMPION_THRESHOLD["accuracy"]
        and test_auc_roc >= CHAMPION_THRESHOLD["auc"]
    )
    meets_challenger = (
        test_accuracy >= CHALLENGER_THRESHOLD["accuracy"]
        and test_auc_roc >= CHALLENGER_THRESHOLD["auc"]
    )
    if meets_champion:
        alias = "champion"
    elif meets_challenger:
        alias = "challenger"

    # TODO: Step 3 - If alias is None, log warning and return None
    if not alias:
        logger.warning("Metrics below thresholds - no alias assigned")
        return None

    # TODO: Step 4 - Set the alias on the model version
    client.set_registered_model_alias(name=model_name, alias=alias, version=version)

    # TODO: Step 5 - Add description with metrics to the model version
    description = f"Accuracy: {test_accuracy}; AUC ROC: {test_auc_roc}"
    client.update_model_version(name=model_name, version=str(version), description=description)

    # TODO: Step 6 - Log success message and return the alias
    logger.info(f"Set alias '{alias}' on version {version}")
    return alias


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
    log_section("Loading data from Feast")
    df = load_training_data_from_feast(feature_service_name=config.get("feast_service_name"))
    selected_features = load_selected_features(config.get("selected_features_path"))
    logger.info(f"Selected features: {len(selected_features)} features")

    log_section("Starting MLflow experiment")

    # Extract MLflow settings from nested dict; fall back to defaults
    tracking_uri = settings.mlflow_tracking_uri
    mlflow_config = config.get("mlflow", {})
    experiment_name = mlflow_config.get("experiment_name", "Stock_Prediction_Experiment")
    run_name = mlflow_config.get("run_name", None)

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment_name)

    # Get model name from config for Model Registry
    model_name = mlflow_config.get("registered_model_name", "stock_prediction_classifier")

    with mlflow.start_run(run_name=run_name) as run:
        run_id = run.info.run_id  # Capture run_id for later use
        logger.info(f"MLflow run ID: {run_id}")
        logger.info(f"MLflow tracking URI: {tracking_uri}")
        logger.info(f"MLflow experiment: {experiment_name}")

        mlflow.log_artifact(config_path or "configs/training/local.yaml", artifact_path="config")
        mlflow.log_params(config.get("model_params", {}))
        mlflow.log_param("selected_feature_count", len(selected_features))

        log_section("Splitting data")
        train_val_df, test_df = split_data_train_test(df, test_size=config.get("test_size", 0.1))
        train_df, val_df = split_data_train_test(train_val_df, test_size=0.5)

        log_section("Encoding categorical features")
        encoder = fit_encoder(train_df)

        # Apply the same encoder to all splits for consistent feature space.
        train_df = transform_with_encoder(train_df, encoder)
        val_df = transform_with_encoder(val_df, encoder)
        test_df = transform_with_encoder(test_df, encoder)

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
        test_metrics = evaluate_model(model, X_test, y_test, prefix="test")  # Capture for promotion
        mlflow.log_metrics(test_metrics)

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

        # 1. Save artifacts locally
        temp_dir = PROJECT_ROOT / "tmp"

        # Get feature names WITHOUT the encoded symbol columns
        raw_features = [f for f in selected_features if not f.startswith("symbol_")]

        artifact_paths = save_model_artifacts_locally(model, encoder, raw_features, temp_dir)

        # 2. Create proper signature for raw input (before encoding)
        # Input: raw features + symbol column
        # Output: predictions dict
        input_columns = raw_features + ["symbol"]

        # Get a sample of raw data (before encoding) for input example
        raw_sample_df = train_val_df.head(5)[input_columns].copy()

        signature = infer_signature(
            model_input=raw_sample_df,
            model_output=pd.DataFrame(
                {
                    "prediction_class": [0],
                    "prediction_proba_up": [0.5],
                    "prediction_proba_down": [0.5],
                }
            ),
        )

        # 3. Log the custom pyfunc model AND register to Model Registry in one step
        model_info = mlflow.pyfunc.log_model(
            name="catboost_model",
            python_model=StockPredictionModel(),
            artifacts=artifact_paths,
            signature=signature,
            input_example=raw_sample_df,
            registered_model_name=model_name,
        )

        # Get the registered model version from model_info
        model_version = model_info.registered_model_version
        logger.info(f"Registered model '{model_name}' version {model_version}")

        log_section("Training complete")
        logger.info(f"View results: mlflow ui --backend-store-uri {tracking_uri}")

    # Promote model with alias based on metrics
    log_section("Promoting model")
    alias = promote_model_with_alias(
        model_name=model_name,
        version=int(model_version),
        test_metrics=test_metrics,
    )

    # Log helpful message for how to load the model:
    if alias:
        logger.info(f"Load with: mlflow.pyfunc.load_model('models:/{model_name}@{alias}')")
    else:
        logger.info(f"Load with: mlflow.pyfunc.load_model('models:/{model_name}/{model_version}')")

    # Clean up resources
    log_section("Clean up")
    clean_up_resources()
    logger.info("Clean up complete!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train a CatBoost model for stock prediction.")
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to the training configuration YAML file.",
    )
    args = parser.parse_args()
    main(args.config)
