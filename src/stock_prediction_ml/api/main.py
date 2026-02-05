"""Stock Prediction API.

This module provides a FastAPI-based REST API for predicting next-day
stock price direction (up/down) using a trained CatBoost model.

Architecture:
    - Model: Loaded from MLflow Model Registry (pyfunc wrapper)
    - Features: Retrieved from Feast online store (SQLite backend)
    - Predictions: Binary classification (0=DOWN, 1=UP) with probabilities

Endpoints:
    GET /health: Check API and dependency status
    POST /predict: Get stock movement prediction for a symbol

Example:
    Start the API:
        $ uvicorn src.stock_prediction_ml.api.main:app --reload

    Make a prediction:
        $ curl -X POST http://localhost:8000/predict \
            -H "Content-Type: application/json" \
            -d '{"symbol": "AAPL", "date": "2026-02-04"}'
"""

import logging
import time
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from pathlib import Path

import mlflow
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from feast import FeatureStore
from mlflow.tracking import MlflowClient

from stock_prediction_ml.api.schema import HealthResponse, PredictionResponse, StockRequest
from stock_prediction_ml.api.utils import check_dependencies
from stock_prediction_ml.config.settings import settings

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
FEAST_REPO_PATH = PROJECT_ROOT / "src" / "stock_prediction_ml" / "feast_repo"
MLRUNS_PATH = PROJECT_ROOT / "mlruns"

# --- Global Variables (loaded on startup) ---
MODEL = None  # pyfunc model (bundles encoder + features internally)
FEAST_STORE = None
MODEL_VERSION = None


def validate_startup() -> None:
    """Validate critical dependencies are loaded.

    Raises:
        RuntimeError: If any critical dependency failed to load.
    """
    missing = []
    if MODEL is None:
        missing.append("MODEL")
    if FEAST_STORE is None:
        missing.append("FEAST_STORE")

    if missing:
        raise RuntimeError(
            f"Failed to load critical dependencies: {', '.join(missing)}. "
            "API cannot start without these."
        )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage API startup and shutdown lifecycle.

    Initializes all dependencies on startup:
        1. MLflow tracking connection
        2. Champion model from Model Registry
        3. Feast online feature store
        4. Model version metadata

    On shutdown, logs a clean exit message.

    Args:
        app: FastAPI application instance.

    Yields:
        None: Control returns to FastAPI to handle requests.

    Raises:
        Logs errors but does not raise - allows partial startup for debugging.
    """
    global MODEL, FEAST_STORE, MODEL_VERSION

    logger.info("=" * 60)
    logger.info("Starting Stock Prediction API...")
    logger.info("=" * 60)

    # Set MLflow tracking URI
    try:
        logger.info("Connecting to MLflow...")
        tracking_uri = f"file://{MLRUNS_PATH}"
        mlflow.set_tracking_uri(tracking_uri)
        client = MlflowClient(tracking_uri=tracking_uri)
    except Exception as e:
        logger.error(f"Failed to connect to MLflow: {e}")

    # Load pyfunc model from Model Registry
    try:
        logger.info("Loading model from Model Registry...")
        model_uri = f"models:/{settings.registered_model_name}@{settings.model_alias}"
        MODEL = mlflow.pyfunc.load_model(model_uri=model_uri)
    except Exception as e:
        logger.error(f"Failed to load model: {e}")

    # Load Feast FeatureStore
    try:
        logger.info(f"Loading Feast store from {FEAST_REPO_PATH}...")
        FEAST_STORE = FeatureStore(str(FEAST_REPO_PATH))
        # Pre-warm registry cache as it frequently expired
        _ = FEAST_STORE.list_feature_views()
        logger.info("Feast registry cache pre-warmed")
    except Exception as e:
        logger.error(f"Failed to load Feast store: {e}")

    # Set MODEL_VERSION from loaded model metadata
    try:
        MODEL_VERSION = client.get_model_version_by_alias(
            name=settings.registered_model_name,
            alias=settings.model_alias,
        ).version
    except Exception as e:
        logger.error(f"Failed to get model version: {e}")
        MODEL_VERSION = "unknown"

    # Validate startup - fail fast if critical deps missing
    try:
        validate_startup()
    except RuntimeError as e:
        logger.critical(str(e))
        raise

    logger.info("=" * 60)
    logger.info("API startup complete!")
    logger.info(f"Model loaded: {MODEL is not None}")
    logger.info(f"Feast store loaded: {FEAST_STORE is not None}")
    logger.info(f"Model version: {MODEL_VERSION}")
    logger.info("=" * 60)

    yield

    logger.info("Shutting down API...")


app = FastAPI(
    title="Stock Prediction API",
    description="API for predicting stock daily movement (up/down)",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next: Callable) -> Response:
    """Log request details and latency for observability."""
    start_time = time.time()

    response = await call_next(request)

    duration = time.time() - start_time
    logger.info(
        f"{request.method} {request.url.path} "
        f"status={response.status_code} duration={duration:.3f}s"
    )

    return response


@app.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """Check API health and dependency status.

    Verifies that all required dependencies are loaded and operational:
        - ML model (from MLflow Model Registry)
        - Feast online feature store

    Returns:
        HealthResponse: Status object containing:
            - status: "healthy" if all deps loaded, else "unhealthy"
            - model_loaded: Boolean indicating model availability
            - feast_online_store: Boolean indicating Feast availability
            - model_version: Current champion model version

    Example:
        GET /health
        Response: {"status": "healthy", "model_loaded": true, ...}
    """
    checker = check_dependencies(MODEL, FEAST_STORE)

    if checker["all_loaded"]:
        status = "healthy"
        logger.info("All dependencies loaded!")
    else:
        status = "unhealthy"
        logger.warning(
            f"Missing dependencies: {', '.join(checker['missing_dependencies'])}"
        )

    # Use the dependencies dict for boolean values
    return HealthResponse(
        status=status,
        model_loaded=checker["dependencies"]["MODEL"],
        feast_online_store=checker["dependencies"]["FEAST"],
        model_version=str(MODEL_VERSION),
    )


@app.post("/predict", response_model=PredictionResponse)
async def predict(request: StockRequest) -> PredictionResponse:
    """Predict next-day stock price movement direction.

    Uses the latest features from Feast online store and the champion
    model from MLflow to predict whether the stock will go UP or DOWN.

    Note:
        The `date` parameter is for logging/reference only. Predictions
        always use the latest available features from the online store.

    Args:
        request: StockRequest containing:
            - symbol: Stock ticker (e.g., "AAPL", "MSFT")
            - date: Reference date (YYYY-MM-DD format)

    Returns:
        PredictionResponse containing:
            - symbol: Requested stock ticker
            - date: Reference date from request
            - prediction: Binary class (0=DOWN, 1=UP)
            - prediction_label: Human-readable label ("UP" or "DOWN")
            - probability: Model confidence for predicted class
            - predicted_at: UTC timestamp of prediction
            - model_version: Version of model used

    Raises:
        HTTPException 503: If model or Feast store not loaded
        HTTPException 500: If feature retrieval or prediction fails
        HTTPException 422: If request validation fails (invalid symbol/date)

    Example:
        POST /predict
        Body: {"symbol": "AAPL", "date": "2026-02-04"}
        Response: {"prediction": 1, "prediction_label": "UP", ...}
    """
    logger.info(f"Received prediction request: {request.symbol} on {request.date}")

    # Check dependencies
    checker = check_dependencies(MODEL, FEAST_STORE)
    if not checker["all_loaded"]:
        raise HTTPException(
            status_code=503,
            detail=f"Missing dependencies: {', '.join(checker['missing_dependencies'])}",
        )

    # Retrieve features from Feast online store
    try:
        logger.info("Retrieving features from Feast online store...")
        entity_rows = [{"symbol": request.symbol}]

        features_df = FEAST_STORE.get_online_features(
            entity_rows=entity_rows,
            features=FEAST_STORE.get_feature_service(settings.feast_service_name),
        ).to_df()
    except Exception as e:
        logger.error(f"Failed to retrieve features: {e}")
        raise HTTPException(
            status_code=500, detail=f"Feature retrieval failed: {str(e)}"
        )

    # Validate features were returned
    # Drop symbol, check if remaining features are all NaN

    # feature_cols steps
    # Step 1: isna Boolean mask (True = NaN, False = value exists)
    # Step 2: all Check if ALL rows in each column are NaN → Series[bool]
    # Step 3: all Check if ALL columns are all-NaN → single bool

    feature_cols = features_df.drop(columns=["symbol"], errors="ignore")
    if features_df.empty or feature_cols.isna().all().all():
        logger.error(f"No features found for symbol: {request.symbol}")
        raise HTTPException(
            status_code=404,
            detail=f"No features available for symbol '{request.symbol}'. "
            "Symbol may not exist or features not materialized.",
        )

    # Cast integer columns (SQLite returns int64, model expects int32)
    for col in settings.int_columns:
        if col in features_df.columns:
            features_df[col] = features_df[col].astype("int32")

    # Make prediction using pyfunc model
    try:
        logger.info("Making prediction...")
        model_prediction_df = MODEL.predict(features_df)
    except Exception as e:
        logger.error(f"Prediction failed: {e}")
        raise HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}")
    
    prediction_class = model_prediction_df['prediction_class'].iloc[0]

    if prediction_class == 1:
        prediction_label = "UP"
        confidence = float(model_prediction_df["prediction_proba_up"].iloc[0])  # P(UP)
    else:
        prediction_label = "DOWN"
        confidence = float(model_prediction_df["prediction_proba_down"].iloc[0])  # P(DOWN)

    logger.info(f"Prediction complete: {prediction_label} (confidence: {confidence:.3f})")

    # Parse pyfunc output and return response
    return PredictionResponse(
        symbol=request.symbol,
        date=request.date,
        prediction=int(prediction_class),
        prediction_label=prediction_label,
        probability=confidence,
        model_version=str(MODEL_VERSION),
    )
