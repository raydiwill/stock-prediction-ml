import logging
import mlflow
import pandas as pd
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from mlflow import MlflowClient

from stock_prediction_ml.api.schema import StockRequest, HealthResponse, PredictionResponse
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

# --- Global Variables (loaded on startup) ---
MODEL = None
ENCODER = None
FEAST_STORE = None
FEAST_SERVICE_NAME = "stock_prediction_service"
SELECTED_FEATURES = None
MODEL_VERSION = "catboost_02012025"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Load all dependencies from MLflow on startup.
    """
    global MODEL, ENCODER, FEAST_STORE, SELECTED_FEATURES, MODEL_VERSION

    logger.info("=" * 60)
    logger.info("Starting Stock Prediction API...")
    logger.info("=" * 60)

    # TODO: Task 4.0 - Set MLflow tracking URI and get experiment
    # Hint: mlflow.set_tracking_uri("file:///path/to/mlruns")
    # Hint: Use PROJECT_ROOT / "mlruns"
    # Hint: Get client with MlflowClient()
    try:
        logger.info("Connecting to MLflow...")
        # YOUR CODE HERE - Set tracking URI
        tracking_uri = mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
        # YOUR CODE HERE - Create MLflow client
        client = MlflowClient(tracking_uri=tracking_uri)
    except Exception as e:
        logger.error(f"Failed to connect to MLflow: {e}")

    # TODO: Task 4.1 - Get latest run from experiment
    # Hint: Use client.search_runs(experiment_ids=[...], order_by=[...])
    # Hint: Experiment name is likely "stock_prediction" (check your train.py)
    # Hint: Order by "start_time DESC" to get latest run
    # Hint: Get run_id from the first result
    try:
        logger.info("Getting latest model run from MLflow...")
        # YOUR CODE HERE - Get experiment by name

        # YOUR CODE HERE - Search runs (limit=1, order by start_time DESC)
        # YOUR CODE HERE - Extract run_id
        pass
    except Exception as e:
        logger.error(f"Failed to get MLflow run: {e}")

    # TODO: Task 4.2 - Download artifacts from MLflow run
    # Hint: Use mlflow.artifacts.download_artifacts(run_id=..., artifact_path=...)
    # Hint: Artifact paths in your train.py: "model/catboost_model.cbm", "metadata/ohe.pkl", "metadata/selected_features.json"
    # Hint: Downloaded files go to mlartifacts/ folder by default
    # Hint: Store paths for later loading
    try:
        logger.info(f"Downloading artifacts from run {run_id}...")
        # YOUR CODE HERE - Download model artifact
        # YOUR CODE HERE - Download encoder artifact
        # YOUR CODE HERE - Download features artifact
        pass
    except Exception as e:
        logger.error(f"Failed to download artifacts: {e}")

    # TODO: Task 4.3 - Load CatBoost model from downloaded artifact
    # Hint: Same as before, but use the downloaded path
    # Hint: CatBoostClassifier().load_model(downloaded_model_path)
    try:
        logger.info("Loading model...")
        # YOUR CODE HERE
        pass
    except Exception as e:
        logger.error(f"Failed to load model: {e}")

    # TODO: Task 4.4 - Load OneHotEncoder from downloaded artifact
    # Hint: pickle.load() from downloaded encoder path
    try:
        logger.info("Loading encoder...")
        # YOUR CODE HERE
        pass
    except Exception as e:
        logger.error(f"Failed to load encoder: {e}")

    # TODO: Task 4.5 - Load selected features from downloaded artifact
    # Hint: json.load() from downloaded features path
    try:
        logger.info("Loading selected features...")
        # YOUR CODE HERE
        pass
    except Exception as e:
        logger.error(f"Failed to load features: {e}")

    # TODO: Task 4.6 - Load Feast FeatureStore (unchanged)
    # Hint: This doesn't come from MLflow, load from FEAST_REPO_PATH
    try:
        logger.info(f"Loading Feast store from {FEAST_REPO_PATH}...")
        # YOUR CODE HERE
        pass
    except Exception as e:
        logger.error(f"Failed to load Feast store: {e}")

    # TODO: Task 4.7 - Set MODEL_VERSION from MLflow run
    # Hint: Use run.data.tags or run.info.run_id
    # YOUR CODE HERE

    logger.info("=" * 60)
    logger.info("API startup complete!")
    logger.info(f"Model loaded: {MODEL is not None}")
    logger.info(f"Encoder loaded: {ENCODER is not None}")
    logger.info(f"Feast store loaded: {FEAST_STORE is not None}")
    logger.info(f"Features loaded: {SELECTED_FEATURES is not None}")
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
    checker = check_dependencies(MODEL, FEAST_STORE, ENCODER, SELECTED_FEATURES)

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
        encoder_loaded=checker["dependencies"]["ENCODER"],
        model_version=MODEL_VERSION,
    )


@app.post("/predict")
async def predict(request: StockRequest):
    """
    Predict next-day stock price direction.
    """
    logger.info(f"Received prediction request: {request.symbol} on {request.date}")

    # Task 3.1 - Check dependencies
    checker = check_dependencies(MODEL, FEAST_STORE, ENCODER, SELECTED_FEATURES)
    if not checker["all_loaded"]:
        raise HTTPException(
            status_code=503,  # Fixed: int, not string
            detail=f"Missing dependencies: {', '.join(checker['missing_dependencies'])}",
        )

    # Task 3.2 - Retrieve features from Feast
    try:
        logger.info("Retrieving features from Feast online store...")
        entity_rows = [{"symbol": request.symbol, "date": request.date}]
        
        features_df = FEAST_STORE.get_online_features(
            entity_rows=entity_rows,
            features=FEAST_STORE.get_feature_service(FEAST_SERVICE_NAME)
        ).to_df()
    except Exception as e:
        logger.error(f"Failed to retrieve features: {e}")
        raise HTTPException(status_code=500, detail=f"Feature retrieval failed: {str(e)}")

    # Task 3.3 - Check if features were found
    if len(features_df) == 0:
        raise HTTPException(status_code=404, detail="No features found!")  # Fixed: int

    # Task 3.4 - Filter to selected features
    features_df = features_df[SELECTED_FEATURES]

    # Task 3.5 - One-hot encode the symbol
    try:
        logger.info("Encoding symbol...")
        symbols_df = pd.DataFrame({"symbol": [request.symbol]})
        matrix_encoded = ENCODER.transform(symbols_df[["symbol"]])
        symbol_columns = ENCODER.get_feature_names_out(["symbol"])
        df_encoded = pd.DataFrame(matrix_encoded, columns=symbol_columns, index=symbols_df.index)
    except Exception as e:
        logger.error(f"Failed to encode symbol: {e}")
        raise HTTPException(status_code=500, detail=f"Encoding failed: {str(e)}")

    # Task 3.6 - Combine features
    pred_df = pd.concat([features_df, df_encoded], axis=1)

    # Task 3.7 - Make prediction
    try:
        logger.info("Making prediction...")
        prediction_class = MODEL.predict(pred_df)[0]  # Get scalar
        prediction_proba = MODEL.predict_proba(pred_df)
        probability_up = float(prediction_proba[0][1])  # P(UP)
    except Exception as e:
        logger.error(f"Prediction failed: {e}")
        raise HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}")

    # Task 3.8 - Return response
    if prediction_class == 1:
        prediction_label = "UP"
        confidence = float(prediction_proba[0][1])  # P(UP)
    else:
        prediction_label = "DOWN"
        confidence = float(prediction_proba[0][0])  # P(DOWN)

    logger.info(f"Prediction complete: {prediction_label} (confidence: {confidence:.3f})")

    return PredictionResponse(
        symbol=request.symbol,
        date=request.date,
        prediction=prediction_label,
        probability=probability_up,
        model_version=MODEL_VERSION,
    )
