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
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path

import mlflow
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from feast import FeatureStore
from mlflow.tracking import MlflowClient

from stock_prediction_ml.api.schema import HealthResponse, PredictionResponse, StockRequest
from stock_prediction_ml.api.utils import check_dependencies

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
REGISTERED_MODEL_NAME = "stock_prediction_classifier"
FEAST_STORE = None
FEAST_SERVICE_NAME = "stock_prediction_service"
MODEL_VERSION = None


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

    # Task 1: Set MLflow tracking URI
    try:
        logger.info("Connecting to MLflow...")
        tracking_uri = f"file://{MLRUNS_PATH}"
        mlflow.set_tracking_uri(tracking_uri)
        client = MlflowClient(tracking_uri=tracking_uri)
    except Exception as e:
        logger.error(f"Failed to connect to MLflow: {e}")

    # Task 2: Load pyfunc model from Model Registry
    # Resource: check local.yaml for registered_model_name
    # Resource: check train.py for alias used during registration
    try:
        logger.info("Loading model from Model Registry...")
        # YOUR CODE HERE - construct model_uri and load
        model_uri = f"models:/{REGISTERED_MODEL_NAME}@champion"
        MODEL = mlflow.pyfunc.load_model(model_uri=model_uri)
    except Exception as e:
        logger.error(f"Failed to load model: {e}")

    # Task 3: Load Feast FeatureStore
    try:
        logger.info(f"Loading Feast store from {FEAST_REPO_PATH}...")
        # YOUR CODE HERE
        FEAST_STORE = FeatureStore(str(FEAST_REPO_PATH))
    except Exception as e:
        logger.error(f"Failed to load Feast store: {e}")

    # Task 4: Set MODEL_VERSION from loaded model metadata
    # YOUR CODE HERE
    try:
        MODEL_VERSION = client.get_model_version_by_alias(
            name=REGISTERED_MODEL_NAME, 
            alias="champion"
        ).version
    except Exception as e:
        logger.error(f"Failed to get model version: {e}")
        MODEL_VERSION = "unknown"

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

    # Task 6.1: Check dependencies
    checker = check_dependencies(MODEL, FEAST_STORE)
    if not checker["all_loaded"]:
        raise HTTPException(
            status_code=503,
            detail=f"Missing dependencies: {', '.join(checker['missing_dependencies'])}",
        )

    # Task 6.2: Retrieve features from Feast online store
    try:
        logger.info("Retrieving features from Feast online store...")
        entity_rows = [{"symbol": request.symbol}]
        
        features_df = FEAST_STORE.get_online_features(
            entity_rows=entity_rows,
            features=FEAST_STORE.get_feature_service(FEAST_SERVICE_NAME)
        ).to_df()
    except Exception as e:
        logger.error(f"Failed to retrieve features: {e}")
        raise HTTPException(status_code=500, detail=f"Feature retrieval failed: {str(e)}")
    
    # Task 6.3: Cast integer columns (SQLite returns int64, model expects int32)
    int_columns = ["day_of_month", "day_of_week", "month"]
    for col in int_columns:
        if col in features_df.columns:
            features_df[col] = features_df[col].astype("int32")

    # Task 6.4: Make prediction using pyfunc model
    try:
        logger.info("Making prediction...")
        # YOUR CODE HERE - pyfunc returns a DataFrame
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

    # Task 6.5: Parse pyfunc output and return response
    # YOUR CODE HERE
    return PredictionResponse(
        symbol=request.symbol,
        date=request.date,
        prediction=int(prediction_class),
        prediction_label=prediction_label,
        probability=confidence,
        model_version=str(MODEL_VERSION),
    )
