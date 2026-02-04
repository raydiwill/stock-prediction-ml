import logging
from contextlib import asynccontextmanager
from pathlib import Path

import mlflow
from mlflow.tracking import MlflowClient
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from feast import FeatureStore

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
REGISTERED_MODEL_NAME = "stock_prediction_classifier"
FEAST_STORE = None
FEAST_SERVICE_NAME = "stock_prediction_service"
MODEL_VERSION = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Load pyfunc model from Model Registry and Feast store on startup.
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


@app.get("/health")
async def health_check():
    """
    Health check endpoint to verify all dependencies are loaded.
    """
    # Task 5: Update check_dependencies call (fewer deps now)
    # YOUR CODE HERE
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


@app.post("/predict")
async def predict(request: StockRequest):
    """
    Predict next-day stock price direction.
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
