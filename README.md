# Daily Stock Prediction with Machine Learning

**Status**: 80% End-to-end pipeline implemented (Pull stock data from API, validation, preprocessing, feature engineering, encoding, training, evaluation).

**Next milestones**: Serve with API -> Containerize into Docker.

A portfolio project that ingests end-of-day (EOD) stock data, engineers features, encodes symbols, trains a CatBoost classifier, evaluates metrics, and saves artifacts. Synthetic fixtures keep tests hermetic in GitHub Actions.

---

## ⚙️ What it does
- Fetches EOD stock data via MarketStack.
- Validates Parquet with Great Expectations.
- Engineers features and persists to `data/feature/stock_eod_features.parquet`.
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
│ └─ meta/                      # ohe.pkl, selected_features.json
├─ notebooks/                   # exploration
├─ src/
│ └─ stock_prediction_ml/
│   ├─ model/
│   │ └─ train.py               # encode → train → eval → save
│   ├─ marketstack/
│   │ └─ pull.py                # ingestion
│   ├─ data_validation/
│   │ └─ validation.py          # Great Expectations checks
│   └─ features/                # feature engineering (WIP)
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

## Usage
Train end-to-end with a config:

```bash
# Run training (time-based split, OHE, CatBoost)
uv run python src/stock_prediction_ml/model/train.py --config configs/training/local.yaml
```

Outputs:
- Encoder: `data/meta/ohe.pkl`
- Model: `models/catboost_model.cbm`
- Metrics: `val_accuracy`, `val_roc_auc`, `test_accuracy`, `test_roc_auc`
- Optional MLflow artifacts: config, features, encoder, model

---

## Configuration
Training YAML fields:
- `training_data_path`: path to features parquet
- `selected_features_path`: JSON `{"features": [...]}` for X columns
- `target`: target column (e.g., `target`)
- `model_params`: CatBoost hyperparameters
- `test_size`: proportion for date-quantile split
- `meta_dir`: directory to save encoder and metadata
- Optional MLflow:
  - `mlflow_tracking_uri`
  - `mlflow_experiment`
  - `model_dir`

Example:
```yaml
training_data_path: data/feature/stock_eod_features.parquet
selected_features_path: data/meta/selected_features.json
target: target
model_params:
  iterations: 300
  depth: 6
  random_seed: 42
test_size: 0.1
meta_dir: data/meta
model_dir: models
mlflow_tracking_uri: file:./mlruns
mlflow_experiment: stock_prediction
```

---

## Testing
Tests generate synthetic data and temp configs, so CI doesn’t require large datasets.

```bash
# Run unit tests
uv run pytest -q
```

Highlights:
- Synthetic fixtures write tiny parquet and JSON to `tmp_path`.
- Encoder tests verify OHE columns and index preservation.
- Split tests assert by date-quantile cutoff (not exact row counts).
- Model saved as `.cbm` and loaded via `CatBoostClassifier().load_model()`.

---

## Design notes
- Temporal splits prevent leakage; quantile cutoff may not match exact row proportions.
- Encoder fit on train only; `handle_unknown=ignore` ensures robust inference.
- Deterministic behavior via `random_seed` in `model_params`.
- Use native CatBoost `.cbm` format (do not load with joblib).

---

## Roadmap

- MLflow registry integration and model CI/CD.
- Deployment (batch scoring or lightweight API).
- Airflow integration.
- Containerize with Docker.
