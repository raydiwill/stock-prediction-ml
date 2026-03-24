"""Batch prediction — run the champion model for a list of tickers and persist results.

Standalone CLI counterpart to the on-demand ``/predict`` API endpoint.
Designed to be called from the Airflow prediction_dag via:

    python -m stock_prediction_ml.model.batch_predict \
        --tickers AAPL MSFT NVDA \
        --date 2026-03-12

Flow:
    1. Load champion pyfunc model from MLflow Model Registry
    2. Fetch features from Feast online store
    3. Validate, clean, and cast feature types
    4. Run model.predict() on full DataFrame
    5. Persist PredictionResult rows to DB
"""

import argparse
import logging
from datetime import UTC, datetime
from pathlib import Path

import mlflow
import numpy as np
import pandas as pd
from feast import FeatureStore
from mlflow.pyfunc import PyFuncModel
from mlflow.tracking import MlflowClient
from sqlalchemy import select

from stock_prediction_ml.config.settings import settings
from stock_prediction_ml.db.models import PredictionResult, RawStockData
from stock_prediction_ml.db.session import SessionLocal

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
FEAST_REPO_PATH = PROJECT_ROOT / "src" / "stock_prediction_ml" / "feast_repo"


def load_champion_model() -> tuple[PyFuncModel, str]:
    """Load the champion model from MLflow Model Registry.

    Returns:
        Tuple of (pyfunc model, model version string).
    """
    logger.info("Initializing MLflow client...")
    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    mlflow_client = MlflowClient(tracking_uri=settings.mlflow_tracking_uri)

    try:
        model_version = mlflow_client.get_model_version_by_alias(
            name=settings.registered_model_name,
            alias=settings.model_alias,
        ).version
    except Exception as e:
        logger.error(f"Failed to get model version: {e}")
        model_version = "unknown"

    model_uri = f"models:/{settings.registered_model_name}@{settings.model_alias}"
    model = mlflow.pyfunc.load_model(model_uri=model_uri)

    # MLflow/Alembic changes root logger level from INFO → WARNING;
    # reset it so propagated messages from this logger are not silenced.
    logging.getLogger().setLevel(logging.INFO)

    return model, model_version


def fetch_features(tickers: list[str]) -> pd.DataFrame:
    """Fetch features from Feast online store for all tickers.

    Drops rows with missing features and casts int64 → int32 for SQLite
    compatibility.

    Args:
        tickers: Stock symbols to fetch features for.

    Returns:
        Cleaned DataFrame ready for model prediction.

    Raises:
        ValueError: If no valid features remain after cleaning.
    """
    logger.info(f"Loading Feast store from {FEAST_REPO_PATH}...")
    feast_store = FeatureStore(str(FEAST_REPO_PATH))
    _ = feast_store.list_feature_views()
    logger.info("Feast registry cache pre-warmed")

    entity_rows = [{"symbol": ticker} for ticker in tickers]
    features_df = feast_store.get_online_features(
        entity_rows=entity_rows,
        features=feast_store.get_feature_service(settings.feast_service_name),
    ).to_df()

    # Validate & clean — drop tickers with NaN features
    if features_df.isna().any().sum() > 0:
        missing_symbols = features_df.loc[features_df.isna().any(axis=1)][
            "symbol"
        ].tolist()
        logger.warning(f"No features found for symbols: {', '.join(missing_symbols)}")
        features_df = features_df.dropna()

    if features_df.empty:
        raise ValueError("No valid features for any ticker.")

    # Cast integer columns (SQLite returns int64, model expects int32)
    for col in settings.int_columns:
        if col in features_df.columns:
            features_df[col] = features_df[col].astype("int32")

    return features_df.reset_index(drop=True)


def run_predictions(model: PyFuncModel, features_df: pd.DataFrame) -> pd.DataFrame:
    """Run model.predict() and merge predictions with features.

    Args:
        model: Loaded MLflow pyfunc model.
        features_df: Cleaned feature DataFrame.

    Returns:
        DataFrame with features and prediction columns merged.
    """
    predictions_df = model.predict(features_df)
    return pd.concat([features_df, predictions_df], axis=1)


def persist_results(
    result_df: pd.DataFrame,
    tickers: list[str],
    date: str,
    model_version: str,
) -> tuple[int, int]:
    """Persist prediction results to the database.

    Args:
        result_df: DataFrame with features and prediction columns.
        tickers: Original ticker list (for DB lookup).
        date: Reference date string (YYYY-MM-DD).
        model_version: Model version string for audit trail.

    Returns:
        Tuple of (success count, failure count).
    """
    prediction_date = pd.to_datetime(date)
    ok, fail = 0, 0
    non_feature_cols = {
        "symbol",
        "prediction_class",
        "prediction_proba_up",
        "prediction_proba_down",
    }

    with SessionLocal() as db:
        query = (
            select(RawStockData)
            .where(RawStockData.symbol.in_(tickers))
            .where(RawStockData.date == prediction_date)
        )
        results = db.execute(query).scalars().all()
        meta_lookup = {row.symbol: row for row in results}

        prediction_results = []
        for _, row in result_df.iterrows():
            symbol = row.symbol
            prediction_class = row.prediction_class

            meta = meta_lookup.get(symbol)
            if meta is None:
                logger.warning(
                    f"No RawStockData row for {symbol} on {date}. "
                    "Prediction will not be persisted."
                )
                fail += 1
                continue

            if prediction_class == 1:
                prediction_label = "UP"
                confidence = float(row["prediction_proba_up"])
            else:
                prediction_label = "DOWN"
                confidence = float(row["prediction_proba_down"])

            feature_cols = [c for c in row.index if c not in non_feature_cols]
            features_dict = {
                k: float(v) if isinstance(v, (int, float, np.number)) else v
                for k, v in row[feature_cols].to_dict().items()
            }

            model_name = f"{settings.registered_model_name}@{settings.model_alias}/v{model_version}"
            prediction_results.append(
                PredictionResult(
                    raw_stock_data_id=meta.id,
                    predicted_at=datetime.now(UTC),
                    model_name=model_name,
                    prediction=int(prediction_class),
                    probability=confidence,
                    features_used=features_dict,
                )
            )

            logger.info(
                f"Prediction for {symbol}: {prediction_label} (confidence: {confidence:.3f})"
            )
            ok += 1

        db.add_all(prediction_results)
        db.commit()

    return ok, fail


def main(tickers: list[str], date: str) -> None:
    """Run batch predictions for *tickers* and persist to the DB.

    Args:
        tickers: Stock symbols to predict (e.g. ["AAPL", "MSFT"]).
        date: Reference date string (YYYY-MM-DD) used to locate the
              corresponding RawStockData row for each ticker.
    """
    logger.info("=" * 60)
    logger.info("Starting batch prediction")
    logger.info(f"  tickers : {tickers}")
    logger.info(f"  date    : {date}")
    logger.info("=" * 60)

    try:
        model, model_version = load_champion_model()
    except Exception as e:
        logger.error(f"Failed to load champion model: {e}")
        return

    try:
        features_df = fetch_features(tickers)
    except Exception as e:
        logger.error(f"Failed to fetch features: {e}")
        return

    result_df = run_predictions(model, features_df)
    ok, fail = persist_results(result_df, tickers, date, model_version)

    logger.info("=" * 60)
    logger.info(
        f"Batch prediction complete: {ok} succeeded, {fail} failed out of {len(tickers)}"
    )
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run batch predictions for configured tickers.",
    )
    parser.add_argument(
        "--tickers",
        nargs="+",
        default=settings.valid_symbols,
        help="Ticker symbols to predict (default: all configured symbols).",
    )
    parser.add_argument(
        "--date",
        type=str,
        required=True,
        help="Reference date YYYY-MM-DD (maps to the RawStockData row).",
    )
    args = parser.parse_args()
    main(args.tickers, args.date)
