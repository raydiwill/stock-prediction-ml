# Project: Stock Price Prediction with Machine Learning

## One-line goal
Build an end-to-end ML pipeline to predict next-day stock price direction (up/down) using EOD data, with focus on reproducible experiments and model lifecycle management.

## Main deliverables
- ✅ **Data ingestion**: MarketStack API integration for EOD stock data (AAPL, MSFT, AMZN, NVDA, GOOGL, META, TSLA)
- ✅ **Data validation**: Great Expectations suite ensuring data quality and schema compliance
- ✅ **Feature engineering**: 30+ technical indicators (returns, lags, rolling stats, RSI, MACD, SMA, time features)
- ✅ **Database layer**: SQLAlchemy models for raw stock data, predictions, and model metadata storage
- ✅ **Model training**: CatBoost classifier with time-series split, one-hot encoding, early stopping
- ✅ **Experiment tracking**: MLflow integration for metrics, artifacts, and model versioning
- ✅ **Model Registry**: MLflow pyfunc wrapper bundling model + encoder + features with alias-based promotion
- ✅ **Testing suite**: Pytest with synthetic fixtures for CI/CD (14+ test modules including API tests)
- ✅ **Feature store**: Feast feature definitions, materialization pipeline, online store (SQLite) with 203+ feature records
- ✅ **REST API**: FastAPI with MLflow Model Registry + Feast online store integration, request logging middleware
- ❌ **Orchestration**: Airflow DAGs not implemented
- ❌ **Monitoring**: Grafana/Prometheus stack not implemented
- 🔄 **UI**: Streamlit dashboard in progress (3-page app: Predictions, Historical Data, About Model)

## Primary stack
**Core**: Python 3.13, pandas, numpy, scikit-learn, CatBoost, XGBoost, LightGBM, RandomForest

**MLOps**: MLflow (experiment tracking + model registry), Great Expectations (data validation), Feast (feature store), pytest

**Database**: SQLAlchemy ORM with SQLite (dev), support for PostgreSQL

**API**: FastAPI (skeleton), Pydantic (schema validation)

**Planned**: Docker, Airflow, Grafana, Streamlit

## Current status
### ✅ Completed (95%) + UI In Progress
1. **Data ingestion** ([`src/stock_prediction_ml/marketstack/pull.py`](src/stock_prediction_ml/marketstack/pull.py))
   - Fetch EOD data from MarketStack API with pagination
   - Save to parquet format in `data/raw/`
   - Combine multiple ticker files into single dataset

2. **Data validation** ([`src/stock_prediction_ml/data_validation/validation.py`](src/stock_prediction_ml/data_validation/validation.py))
   - Great Expectations suite with 15+ expectations
   - Schema validation, null checks, type checks
   - Logical price relationships (high > low)
   - Compound uniqueness (date + symbol)
   - Saves validated data to `data/processed/validated_data.parquet`

3. **Feature engineering** ([`src/stock_prediction_ml/features/build_features.py`](src/stock_prediction_ml/features/build_features.py))
   - **Target**: Binary classification (1 if next close > current close)
   - **Technical features** (30+ features):
     - Returns and lagged returns (1, 2, 5, 10 days)
     - Rolling statistics (mean, std for 5, 10-day windows)
     - Range features (high-low, close-open)
     - Time features (day_of_week, month, quarter, day_of_month)
     - Technical indicators (SMA 10/20, RSI 14, EMA 12/26, MACD, MACD signal)
   - Handles NaN with 20-row warm-up drop
   - Outputs to `data/feature/stock_eod_features.parquet`

4. **Database models** ([`src/stock_prediction_ml/db/models.py`](src/stock_prediction_ml/db/models.py))
   - `RawStockData`: stores ingested stock data with hash-based deduplication
   - `PredictionResult`: stores model predictions with metadata
   - Ingestion pipeline ([`src/stock_prediction_ml/db/ingest.py`](src/stock_prediction_ml/db/ingest.py)) with adaptive batching

5. **Model training** ([`src/stock_prediction_ml/model/train.py`](src/stock_prediction_ml/model/train.py))
   - **Data Loading**:
     - `load_training_data_from_feast()`: Retrieves historical features from Feast offline store
     - Point-in-time correct joins via `get_historical_features()` API
     - Configurable feature service (default: `stock_training_service`)
   - **Preprocessing**:
     - Time-based train/val/test split using date quantiles (default 90/5/5)
     - `fit_encoder()`: Fits OneHotEncoder on train only (in-memory, no disk save)
     - `transform_with_encoder()`: Applies encoder to any split
     - Selected features loaded from `data/meta/selected_features.json`
   - **Training**:
     - CatBoost classifier with early stopping (50 rounds on validation set)
     - Configurable hyperparameters via YAML ([`configs/training/local.yaml`](configs/training/local.yaml))
     - Deterministic with `random_seed: 42`, `allow_writing_files: false` in tests
   - **Evaluation**:
     - Metrics: accuracy and ROC-AUC for both validation and test sets
     - Confusion matrix, ROC curve, feature importance plots
   - **MLflow integration**:
     - Logs params, metrics, artifacts (config, diagnostics)
     - Custom pyfunc model with bundled artifacts (see Model Registry below)

6. **Model Registry** ([`src/stock_prediction_ml/model/train.py`](src/stock_prediction_ml/model/train.py))
   - **Custom PyFunc Wrapper** (`StockPredictionModel` class):
     - Bundles CatBoost model + OneHotEncoder + selected features into single artifact
     - `load_context()`: Loads all dependencies from artifact paths
     - `predict()`: Applies encoding + prediction in one step (API-ready)
   - **Artifact Bundling** (`save_model_artifacts_locally()`):
     - Saves model as native `.cbm` format
     - Saves encoder as `joblib` pickle
     - Saves feature list as JSON
     - Returns artifact paths dict for `mlflow.pyfunc.log_model()`
   - **Registration**:
     - `mlflow.pyfunc.log_model(..., registered_model_name=...)` registers in one step
     - Model URI: `models:/{model_name}/{version}` or `models:/{model_name}@{alias}`
   - **Alias-based Promotion** (`promote_model_with_alias()`):
     - `champion`: test_accuracy >= 0.65 AND test_roc_auc >= 0.70
     - `challenger`: test_accuracy >= 0.60 AND test_roc_auc >= 0.65
     - Adds description with metrics to model version
   - **API Loading Pattern**:
     ```python
     model = mlflow.pyfunc.load_model("models:/stock_prediction_classifier@champion")
     predictions = model.predict(raw_input_df)  # Encoding handled internally
     ```

7. **Experimentation** (notebooks 01-06)
   - [`notebooks/01_api_pull.ipynb`](notebooks/01_api_pull.ipynb): API exploration and data pull
   - [`notebooks/02_data_validation.ipynb`](notebooks/02_data_validation.ipynb): Great Expectations setup
   - [`notebooks/04_feature_engineering.ipynb`](notebooks/04_feature_engineering.ipynb): Feature creation prototypes
   - [`notebooks/05_baseline.ipynb`](notebooks/05_baseline.ipynb): Baseline models and feature selection (permutation importance)
   - [`notebooks/06_tuning.ipynb`](notebooks/06_tuning.ipynb): Optuna hyperparameter tuning for RF, XGBoost, LightGBM, CatBoost

8. **Testing** ([`tests/`](tests/))
   - Synthetic fixtures for hermetic CI tests
   - Pytest markers: `@pytest.mark.slow` for integration tests.
   - Test coverage:
     - Data pull and processing ([`test_pull.py`](tests/test_pull.py))
     - Data validation expectations ([`test_validation.py`](tests/test_validation.py))
     - Feature engineering ([`test_build_features.py`](tests/test_build_features.py))
     - Database ingestion ([`test_ingest.py`](tests/test_ingest.py))
     - Training pipeline ([`test_train.py`](tests/test_train.py)) - 26 tests including Feast data loading
     - Feast feature store ([`test_feast.py`](tests/test_feast.py)) - Entity/view schemas, materialization, retrieval
     - REST API endpoints ([`test_api.py`](tests/test_api.py)) - 8 tests with mocked MLflow/Feast dependencies

9. **Feature Store** ([`src/stock_prediction_ml/feast_repo/`](src/stock_prediction_ml/feast_repo/))
   - **Infrastructure**:
     - Feast SDK v0.50.0+ configured with file-based offline store (parquet + DuckDB)
     - SQLite online store for low-latency feature serving
     - Local registry (`data/feast/registry.db`) for feature metadata
   - **Entity definition** ([`entities.py`](src/stock_prediction_ml/feast_repo/entities.py)):
     - `symbol` entity (STRING type) as join key for stock ticker
   - **Feature views** ([`features_definition.py`](src/stock_prediction_ml/feast_repo/features_definition.py)):
     - `stock_basic_features`: 6 OHLCV features (open, high, low, close, adj_close, volume)
     - `stock_technical_features`: 18 technical indicators (returns, lags, rolling stats, RSI, MACD, SMA)
     - `stock_timeseries_features`: 5 temporal features (day_of_week, month, quarter)
     - `stock_target_label`: Binary target for model training (online=False)
   - **Feature services** ([`feature_services.py`](src/stock_prediction_ml/feast_repo/feature_services.py)):
     - `stock_prediction_service`: Bundles feature views for model serving (excludes target)
     - `stock_training_service`: Bundles all features including target for training pipeline
   - **Training Integration**:
     - `load_training_data_from_feast()` function retrieves historical features via offline store
     - Point-in-time correct joins ensure no data leakage
     - Training pipeline fully migrated from direct parquet loading to Feast API
   - **Materialization**:
     - Successfully materialized 203+ feature records to online store
     - Data range: 2024-2025 EOD stock data
     - Online store ready for real-time feature retrieval in API endpoints

10. **REST API** ([`src/stock_prediction_ml/api/main.py`](src/stock_prediction_ml/api/main.py))
   - **Lifecycle Management** (`lifespan` function):
     - Startup: Loads champion model from MLflow Registry (`models:/stock_prediction_classifier@champion`)
     - Startup: Initializes Feast FeatureStore with registry cache pre-warming
     - Startup: Validates critical dependencies (fail-fast on missing deps via `validate_startup()`)
     - Shutdown: Clean exit logging
   - **Endpoints**:
     - `GET /health`: Returns model/Feast availability status and model version
     - `POST /predict`: Retrieves features from Feast online store → model prediction with confidence scores
   - **Implementation Details**:
     - Model loaded via `mlflow.pyfunc.load_model()` with alias-based lookup
     - Feature retrieval via `FEAST_STORE.get_online_features(features=feature_service)`
     - Type casting for SQLite int64 → int32 compatibility (model signature requirement)
     - Pyfunc handles symbol encoding internally (no manual encoder needed)
     - Feature validation: Returns 404 if Feast returns empty/null features
   - **Middleware**:
     - CORS enabled for all origins
     - Request logging middleware tracking method, path, status, and latency
   - **Configuration** ([`src/stock_prediction_ml/config/settings.py`](src/stock_prediction_ml/config/settings.py)):
     - Centralized settings for model name, alias, Feast service, int columns
     - All hardcoded constants migrated to `settings` object
   - Schema definitions ([`src/stock_prediction_ml/api/schema.py`](src/stock_prediction_ml/api/schema.py))

### 🔄 In Progress: Streamlit UI
**Goal:** Recruiter-ready dashboard for stock prediction demos

**File Structure:**
```
src/stock_prediction_ml/streamlit_app/
├── app.py                    # Entry point
├── pages/
│   ├── 1_Predictions.py      # Symbol selector, date picker, predict button, result
│   ├── 2_Historical_Data.py  # Candlestick charts with volume
│   └── 3_About_Model.py      # Model metrics, feature importance, tech stack
├── components/
│   ├── api_client.py         # HTTP wrapper for FastAPI calls (predict, health_check)
│   └── charts.py             # Plotly chart builders (candlestick, confidence gauge)
└── utils/
    └── data_loader.py        # Parquet readers for historical data
```

**Implementation Phases:**
1. **MVP** - Add dependencies → api_client → app.py → 1_Predictions.py
2. **Visual Polish** - data_loader → charts.py → 2_Historical_Data.py → confidence gauge
3. **Model Showcase** - 3_About_Model.py with MLflow artifacts
4. **Production Prep** - Error handling, theme config, Streamlit Cloud deployment

**Dependencies to add:** `streamlit>=1.28.0`, `plotly>=5.18.0`

**Run commands:**
- API: `uvicorn src.stock_prediction_ml.api.main:app --reload`
- UI: `streamlit run src/stock_prediction_ml/streamlit_app/app.py`

### ❌ Not Started
1. **Orchestration**: Airflow DAGs for automated pipeline execution
2. **Monitoring**: Grafana dashboards for drift detection and performance tracking
3. **Deployment**: Docker containerization, model serving infrastructure

## Key constraints
- **Time-series integrity**: Strict temporal train/test splits via date quantiles (no data leakage)
- **Reproducibility**: Fixed random seeds, MLflow tracking, synthetic test fixtures
- **Memory efficiency**: DuckDB/Postgres for offline store (local dev compatible)
- **Open-source first**: No proprietary feature stores or cloud-only solutions
- **CI/CD ready**: All tests run in GitHub Actions without external data dependencies
- **Model format**: Native CatBoost `.cbm` wrapped in MLflow pyfunc for self-contained serving

## Important files
### Core modules
- [`src/stock_prediction_ml/marketstack/pull.py`](src/stock_prediction_ml/marketstack/pull.py): API ingestion with pagination and parquet output
- [`src/stock_prediction_ml/data_validation/validation.py`](src/stock_prediction_ml/data_validation/validation.py): Great Expectations validation suite
- [`src/stock_prediction_ml/features/build_features.py`](src/stock_prediction_ml/features/build_features.py): Feature engineering pipeline
- [`src/stock_prediction_ml/model/train.py`](src/stock_prediction_ml/model/train.py): Training + Model Registry with pyfunc bundling
- [`src/stock_prediction_ml/db/ingest.py`](src/stock_prediction_ml/db/ingest.py): Database ingestion with deduplication
- [`src/stock_prediction_ml/feast_repo/`](src/stock_prediction_ml/feast_repo/): Feature store infrastructure (Feast SDK)
- [`src/stock_prediction_ml/api/main.py`](src/stock_prediction_ml/api/main.py): FastAPI application with MLflow + Feast integration
- [`src/stock_prediction_ml/streamlit_app/`](src/stock_prediction_ml/streamlit_app/): Streamlit UI (in progress)

### Configuration
- [`configs/training/local.yaml`](configs/training/local.yaml): Training configuration (paths, hyperparameters, Feast service, MLflow settings)
- [`src/stock_prediction_ml/feast_repo/feature_store.yaml`](src/stock_prediction_ml/feast_repo/feature_store.yaml): Feast feature store configuration (offline/online stores, registry)
- [`config.env`](config.env): Environment variables (API keys, DB URL, MLflow URI)
- [`pyproject.toml`](pyproject.toml): Project dependencies and metadata

### Notebooks
- [`notebooks/05_baseline.ipynb`](notebooks/05_baseline.ipynb): Feature selection with permutation importance
- [`notebooks/06_tuning.ipynb`](notebooks/06_tuning.ipynb): Optuna hyperparameter optimization

## Example I/O
### Input
Raw EOD data from MarketStack API:
```python
{
    'symbol': 'AAPL',
    'date': '2024-01-01',
    'open': 185.50,
    'high': 188.20,
    'low': 184.90,
    'close': 187.30,
    'volume': 52000000,
    'adj_close': 187.30
}
```

### Output
Feature vector (34 features after encoding `symbol`):
```python
{
    'symbol_AAPL': 1,        # One-hot encoded
    'symbol_MSFT': 0,
    ...,
    'return': 0.0097,        # (close - prev_close) / prev_close
    'return_lag_1': 0.0055,
    'return_roll_mean_5': 0.0082,
    'rsi_14': 62.3,
    'macd': 0.45,
    'day_of_week': 0,        # Monday
    'target': 1              # Next close > current close
}
```

### Model prediction
```python
{
    'prediction': 1,          # Up
    'probability': 0.647
}
```

## Acceptance tests (automated)
✅ **Data ingestion**:
- `test_fetch_ticker_data_returns_list_of_dicts`: API response parsing
- `test_process_dataframe_columns_exist`: Required columns present after processing
- `test_save_to_parquet`: Valid parquet creation with correct schema

✅ **Data validation**:
- `test_suite_has_expected_expectations`: All 15+ expectations registered
- `test_validate_batch_returns_validation_result`: Batch validation executes successfully

✅ **Feature engineering**:
- `test_create_features`: All 30+ features created without NaNs
- `test_read_validated_data_reads_parquet`: Parquet loading with date conversion

✅ **Database**:
- `test_ingest_data_into_db_returns_summary`: Ingestion completes with deduplication
- `test_read_validated_file_default_path`: Default path resolution works

✅ **Training**:
- `test_load_config_returns_dict_with_expected_keys`: Config parsing validates
- `test_load_training_data_from_feast_*`: Feast offline store data loading (3 tests)
- `test_fit_encoder_returns_encoder`: Encoder fits correctly (in-memory)
- `test_transform_with_encoder_adds_ohe_columns`: Encoding applied correctly
- `test_train_model`: Model trains and achieves >50% accuracy on synthetic data
- `test_evaluate_model`: Metrics (accuracy, ROC-AUC) computed correctly

✅ **Feast Feature Store**:
- `test_entity_has_correct_attributes`: Entity configuration validated
- `test_stock_*_features_has_correct_schema`: Feature view schemas validated (basic, technical, timeseries)
- `test_feature_service_includes_all_views`: Feature service bundling verified
- `test_feature_store_can_be_initialized`: FeatureStore initialization successful
- `test_feature_store_lists_entities`: Entity registry listing works
- `test_feature_store_lists_feature_views`: Feature view discovery works
- `test_materialize_features_to_online_store`: Materialization from offline to online store
- `test_get_online_features_returns_correct_values`: Online feature retrieval for serving
- `test_get_historical_features_performs_point_in_time_join`: Historical features with point-in-time correctness
- `test_get_online_features_using_feature_service`: Feature service-based retrieval

✅ **REST API**:
- `test_health_returns_healthy_when_all_dependencies_loaded`: Health check with all deps
- `test_health_returns_unhealthy_when_model_missing`: Health check with missing model
- `test_predict_success_returns_prediction`: Valid prediction request
- `test_predict_missing_features_returns_404`: Missing Feast features
- `test_predict_model_failure_returns_500`: Model prediction error handling
- `test_predict_dependencies_missing_returns_503`: Service unavailable when deps missing
- `test_invalid_symbol_returns_422`: Request validation for invalid symbols
- `test_weekend_date_returns_422`: Request validation for weekend dates

## Notes / decisions
### Architecture
- **Temporal splits over random splits**: Date quantiles (90/5/5) prevent leakage in time-series data
- **Native CatBoost format**: `.cbm` files avoid pickle compatibility issues across Python versions
- **MLflow for lineage**: Tracks experiments, models, and artifacts in local filesystem (upgradeable to remote)
- **Bundled artifacts**: Encoder + features bundled with model in pyfunc (simplifies API loading)

### Model Registry approach
- **Pyfunc wrapper**: `StockPredictionModel` class encapsulates all inference dependencies
- **Artifact bundling**: Model, encoder, and feature list saved to temp dir, then logged as single artifact
- **Alias-based promotion**: `champion`/`challenger` aliases instead of stages (MLflow 2.9+ pattern)
- **API simplification**: Single `mlflow.pyfunc.load_model()` call replaces 3 separate artifact downloads

### Data pipeline
- **Deduplication via hashing**: `(symbol, date, ohlc, volume)` hashed to prevent duplicate inserts
- **Great Expectations**: Validates schema, nulls, types, price logic, and compound uniqueness
- **Feature warmup**: Drops first 20 rows to eliminate NaNs from lagged/rolling features

### Model selection
- **CatBoost chosen**: Outperformed RandomForest, XGBoost, LightGBM in notebook experiments (accuracy ~0.54 on test)
- **Early stopping**: 50 rounds on validation set prevents overfitting
- **Feature selection**: Top 20 features by permutation importance (reduces overfitting, improves speed)

### Feature store approach
- **Feast for serving**: Separates feature engineering (batch) from feature serving (online/offline)
- **File-based offline store**: DuckDB queries on parquet files (simple, cost-effective for learning)
- **SQLite online store**: Local development setup (upgradeable to Redis for production)
- **Point-in-time correctness**: Feast ensures training features match serving features (no train/serve skew)
- **Entity-based design**: `symbol` entity enables join-key lookups for per-stock features
- **Materialization workflow**: Full materialize for backfills, incremental for daily updates
- **Module organization**: Single `features_definition.py` prevents import duplication (Feast scans all `.py` files independently)

### Testing strategy
- **Synthetic fixtures**: Generate minimal parquet/JSON in `tmp_path` (no large files in repo)
- **Hermetic tests**: Zero external dependencies (no API calls, no database writes in CI)
- **Deterministic**: Fixed random seeds ensure reproducible test outcomes

### Future tech debt
- **API integration tests**: Add `@pytest.mark.integration` tests with real Feast online store (not mocked)
- **API data materialization**: Online store needs recent date coverage for production readiness
- **Production Feast tests**: Add integration tests using production feature store (not temp fixtures)
- **Incremental materialization**: Implement daily `materialize-incremental` workflow for feature updates
- **Airflow integration**: Manual CLI execution; needs orchestration for daily retraining
- **Monitoring**: No drift detection or performance tracking in production (Grafana/Prometheus planned)
- **Containerization**: Docker Compose for local dev and production deployment

---

## Coding Instructions for Future Development

### Role Definition
You will act as my **coding companion** throughout this project. We will code alongside each other with the primary goal of completing the remaining deliverables (REST API integration, orchestration, monitoring, UI) while prioritizing **learning over speed**.

### Workflow (5-Step Process)

#### Step 1: Introduction & Skeleton Code
- Provide **high-level overview** of the task/concept
- Share **skeleton code with hints** (not full implementation)
- Explain **what needs to be coded** without giving away the solution
- **Hint Philosophy**:
  - Provide **one full example** per concept (first occurrence only)
  - Subsequent uses: show **pattern only**, not specific values
  - Point to **documentation/resources** instead of direct answers
  - Encourage **discovery and experimentation**
- Goal: Help me understand the structure so I can write the implementation myself

#### Step 2: Debugging Support
- Once I write my code and run it, **act as my debugger** if I encounter:
  - Bugs or errors
  - Unexpected behavior
  - Performance issues
- Help me diagnose and fix issues to make the script **fully functional**
- Explain **why** the error occurred and **how** the fix resolves it

#### Step 3: Code Review & Refactoring (QA Role)
- Once code is functional, **act as QA engineer**
- Refactor my code to match:
  - **Production best practices** (error handling, logging, type hints)
  - **Real-world patterns** (dependency injection, separation of concerns)
  - **MLOps standards** (experiment tracking, reproducibility)
- **Explain why** each optimization is made (educational reasoning, not just "this is better")

#### Step 4: Testing
- Provide **test skeleton** (structure + hints)
- I write the test implementation
- Debug tests together if needed
- Refactor tests to match best practices (fixtures, parametrize, mocking)

#### Step 5: Documentation & Knowledge Check
- Generate **comprehensive documentation**:
  - Docstrings (Google or NumPy style)
  - README updates if needed
  - Inline comments for complex logic
- Provide **3 quiz questions** to validate my understanding of:
  - Core concepts
  - Implementation decisions
  - Production considerations

### Quality Standards

#### 1. Credible Sources Only
- **No assumptions or hallucinations** - cite official documentation when making claims
- Reference authoritative sources:
  - Official library docs (Feast, FastAPI, MLflow, etc.)
  - Research papers (for ML concepts)
  - Industry best practices (Google SRE, Uber ML platform, etc.)
- If uncertain, explicitly state: *"I'm not certain, but based on [source]..."*

#### 2. Clear & Concise Communication
- Keep explanations **brief but complete**
- Use **bullet points** for lists
- Highlight **key takeaways** in bold
- Avoid unnecessary jargon (or define it when first used)

#### 3. ELI5 (Explain Like I'm 5)
- When answering conceptual questions, start with **simple analogies**
- Example: "Materialization is like copying recipes from a cookbook to pocket cards"
- Then provide technical details for depth

#### 4. Code Modifications (Not New Files)
- **Default behavior**: Modify existing files using `replace_string_in_file`
- Only create new files if:
  - Explicitly requested by me
  - Required by framework conventions (e.g., new test module)
- Preserve existing code structure and style

### Examples of Good vs. Bad Responses

#### ❌ Bad: Too Specific (Gives Away Answer)
```python
# TODO: Define entity rows
# Hint: entity_rows is a dict like {"symbol": ["AAPL", "MSFT"]}
entity_rows = {
    ???: [???, ???]  # Hint: Use "symbol" as key, add "AAPL" and "MSFT"
}
```

#### ✅ Good: Balanced Hints (First Occurrence)
```python
# TODO: Define entity rows
# Format: {"entity_name": ["value1", "value2"]}
# Example: {"symbol": ["AAPL"]}
# Resources: Check your entities.py for entity name
entity_rows = {
    ???: [???, ???]  # TODO: Add 2 stock symbols to test
}
```

#### ✅ Good: Minimal Hints (Subsequent Uses)
```python
# TODO: Define entity rows for another test
# Hint: Same pattern as previous test
entity_rows = ???

# TODO: Define feature references
# Format: "view_name:feature_name"
# Resources: Check features_definition.py for available features
features = [???, ???]
```

#### ❌ Bad: "This is the right way"
```python
# Use this logging configuration (trust me):
logging.basicConfig(level=logging.INFO)
```

#### ✅ Good: Explain Why
```python
# Use structured logging for production systems
# Why? Enables log aggregation tools (ELK, Splunk) to parse fields
# Why INFO level? Balance between visibility and noise (DEBUG too verbose)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
```

### Communication Preferences
- **Be concise**: Aim for <200 words per explanation unless depth is needed
- **Use analogies**: Real-world comparisons for abstract concepts
- **Show examples**: Code snippets > lengthy descriptions
- **Highlight tradeoffs**: "Option A is faster but less maintainable, Option B is..."
- **Confirm understanding**: End complex explanations with: "Does this make sense?" or "Ready to try implementing?"