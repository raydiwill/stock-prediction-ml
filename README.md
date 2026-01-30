# Daily Stock Prediction with Machine Learning

**Status**: 85% End-to-end pipeline implemented (Pull stock data from API, validation, preprocessing, feature engineering, **Feast feature store**, encoding, training, evaluation).

**Next milestones**: Feast online store → REST API integration → Containerize into Docker.

A portfolio project that ingests end-of-day (EOD) stock data, engineers features, stores them in Feast feature store, trains a CatBoost classifier with point-in-time correct feature retrieval, and tracks experiments via MLflow. Synthetic fixtures keep tests hermetic in GitHub Actions.

---

## ⚙️ What it does
- Fetches EOD stock data via MarketStack API.
- Validates Parquet with Great Expectations.
- Engineers 30+ features (technical indicators, lags, rolling stats, time features).
- **Stores features in Feast feature store** (offline + online stores).
- **Loads training data from Feast offline store** with point-in-time correctness.
- OneHotEncoder for `symbol` saved as `data/meta/ohe.pkl`.
- Time-based train/val/test split via date quantiles (no leakage).
- Trains CatBoost with early stopping and reproducible `random_seed`.
- Evaluates accuracy and ROC-AUC (prefixed `val_` and `test_`).
- Saves model to native `.cbm` and logs artifacts via MLflow.

---

## 📁 Project layout
```text
├─ data/
│ ├─ raw/                       # pulled EOD parquet
│ ├─ feature/                   # engineered features parquet
│ ├─ feast/                     # Feast registry and online store
│ └─ meta/                      # ohe.pkl, selected_features.json
├─ notebooks/                   # exploration
├─ src/
│ └─ stock_prediction_ml/
│   ├─ model/
│   │ └─ train.py               # Feast load → encode → train → eval
│   ├─ feast_repo/              # Feast feature definitions
│   │ ├─ entities.py            # Stock symbol entity
│   │ ├─ features_definition.py # Feature views (basic, technical, timeseries, target)
│   │ ├─ feature_services.py    # Serving + training services
│   │ └─ feature_store.yaml     # Feast configuration
│   ├─ marketstack/
│   │ └─ pull.py                # ingestion
│   ├─ data_validation/
│   │ └─ validation.py          # Great Expectations checks
│   └─ features/
│     └─ build_features.py      # feature engineering
├─ tests/                       # synthetic, hermetic tests
├─ configs/                     # training configs (local/example)
├─ .gitignore
├─ pyproject.toml
├─ uv.lock
└─ README.md
```

---

## 🧮 Prerequisites
- Python 3.13+
- Recommended: `uv` for environment management
- MarketStack API key (for data ingestion module)

---

## 🧰 Installation
```bash
# Clone repo
git clone https://github.com/raydiwill/stock-prediction-ml.git
cd stock-prediction-ml

# Setup environment and install
uv sync
uv run pip install -e .
```

---

## 🚀 Usage

### 1. Apply Feast Feature Store
```bash
# Register feature definitions (first time only)
cd src/stock_prediction_ml/feast_repo
feast apply

# Verify feature views
feast feature-views list
```

### 2. Train Model
```bash
# Run training with Feast data loading
uv run python src/stock_prediction_ml/model/train.py --config configs/training/local.yaml
```

Outputs:
- Encoder: `data/meta/ohe.pkl`
- Model: `models/catboost_model.cbm`
- Metrics: `val_accuracy`, `val_roc_auc`, `test_accuracy`, `test_roc_auc`
- MLflow artifacts: config, features, encoder, model, plots

---

## ⚙️ Configuration
Training YAML fields:
- `feast_service_name`: Feast feature service for training (e.g., `stock_training_service`)
- `selected_features_path`: JSON `{"features": [...]}` for X columns
- `target`: target column (e.g., `target`)
- `model_params`: CatBoost hyperparameters (include `allow_writing_files: false` for tests)
- `test_size`: proportion for date-quantile split
- `meta_dir`: directory to save encoder and metadata
- MLflow:
  - `mlflow_tracking_uri`
  - `mlflow_experiment`
  - `model_dir`

Example:
```yaml
feast_service_name: stock_training_service
selected_features_path: data/meta/selected_features.json
target: target
model_params:
  iterations: 300
  depth: 6
  random_seed: 42
  allow_writing_files: false  # Prevents catboost_info/ in tests
test_size: 0.1
meta_dir: data/meta
model_dir: models
mlflow_tracking_uri: file:./mlruns
mlflow_experiment: Stock_Prediction_Experiment
```

---

## 🧪 Testing
Tests generate synthetic data and temp configs, so CI doesn't require large datasets.

```bash
# Run fast tests only (default)
uv run pytest -m "not slow" -q

# Run all tests including Feast integration
uv run pytest -q
```

Highlights:
- Synthetic fixtures write tiny parquet and JSON to `tmp_path`.
- Feast tests create temporary feature repos with Python API (no CLI dependencies).
- Encoder tests verify OHE columns and index preservation.
- Split tests assert by date-quantile cutoff (not exact row counts).
- Model saved as `.cbm` and loaded via `CatBoostClassifier().load_model()`.
- Test markers: `@pytest.mark.slow` for integration tests (Feast setup).

---

## 📝 Design notes
- **Feast feature store**: Offline store (DuckDB + parquet) for training, online store (SQLite) for serving.
- **Point-in-time correctness**: `get_historical_features()` prevents data leakage during training.
- **Temporal splits**: Date-quantile cutoff prevents leakage; may not match exact row proportions.
- **Encoder**: Fit on train only; `handle_unknown=ignore` ensures robust inference.
- **Deterministic behavior**: `random_seed` in `model_params` + `allow_writing_files=false` in tests.
- **Model format**: Native CatBoost `.cbm` (do not load with joblib).
- **Feature services**: `stock_training_service` (includes target), `stock_prediction_service` (serving only).

---

## 🗺️ Roadmap

**Completed:**
- ✅ Feast feature store integration (offline store → training pipeline)
- ✅ Point-in-time correct feature retrieval
- ✅ MLflow experiment tracking

**In Progress:**
- 🚧 Feast online store → REST API integration (predict endpoint)

**Planned:**
- ⏳ Model registry and CI/CD pipeline
- ⏳ Airflow orchestration (daily retraining)
- ⏳ Docker containerization
- ⏳ Monitoring (Grafana/Prometheus)
