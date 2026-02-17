# Stock Price Prediction ML Pipeline

> End-to-end machine learning system for predicting next-day stock price direction using production-grade MLOps practices.

![Python](https://img.shields.io/badge/Python-3.13-blue)
![MLflow](https://img.shields.io/badge/MLflow-Model%20Registry-orange)
![Feast](https://img.shields.io/badge/Feast-Feature%20Store-green)
![FastAPI](https://img.shields.io/badge/FastAPI-REST%20API-teal)
![Status](https://img.shields.io/badge/Status-92%25%20Complete-yellow)

---

## Status

**Current Stage**: Core ML pipeline complete. REST API implemented with MLflow Model Registry + Feast online store integration.

**Next Step**: Write comprehensive API tests with mocked dependencies, then containerize with Docker Compose.

| Component | Status |
|-----------|--------|
| Data Ingestion | ✅ Complete |
| Data Validation | ✅ Complete |
| Feature Engineering | ✅ Complete |
| Feature Store (Feast) | ✅ Complete |
| Model Training + Registry | ✅ Complete |
| REST API | ✅ Complete |
| Airflow | 🚧 In Progress |
| Grafana | ⏳ Planned |
| UI | ⏳ Planned |
| Docker/Orchestration | ⏳ Planned |

---

## Introduction

A **portfolio project** demonstrating production ML practices for financial time-series prediction.

**What it does:**
- Ingests end-of-day stock data (AAPL, MSFT, GOOGL, etc.) via MarketStack API
- Engineers 30+ technical indicators (RSI, MACD, SMA, rolling stats, lag features)
- Stores features in **Feast feature store** with point-in-time correctness
- Trains a **CatBoost classifier** to predict if tomorrow's close > today's close
- Tracks experiments and registers models via **MLflow**
- Serves predictions through a **FastAPI** endpoint with real-time feature retrieval

**Why this project?** To try and get rich! *(But seriously speaking, I want to apply my uni knowledge and tutorial hells into practical projects, something I can used in daily stock trading)*

---

## Live Demo

> **Coming Soon** — A live demo will be available once the project is containerized and deployed. Check back for a link to test the prediction API!

---

## Tech Stack

### Core ML
| Technology | Purpose |
|------------|---------|
| **CatBoost** | Gradient boosting classifier (chosen after benchmarking RF, XGBoost, LightGBM) |
| **scikit-learn** | Preprocessing, encoding, evaluation metrics |
| **pandas / NumPy** | Data manipulation and feature engineering |

### MLOps
| Technology | Purpose |
|------------|---------|
| **MLflow** | Experiment tracking, model registry, artifact versioning |
| **Feast** | Feature store (offline for training, online for serving) |
| **Great Expectations** | Data validation and quality checks |
| **pytest** | Testing with synthetic fixtures (hermetic CI) |

### API & Infrastructure
| Technology | Purpose |
|------------|---------|
| **FastAPI** | REST API for model serving |
| **SQLAlchemy** | ORM for prediction logging and model metadata |
| **Pydantic** | Request/response validation |
| **Docker** *(planned)* | Containerization for deployment |

---

## Lessons Learned

### 1. Test-Driven Mindset
- Wrote test cases for each script before considering it "done"
- Promotes confidence when refactoring and catches bugs before production
- Building this habit early pays off in maintainability

### 2. Clean Project Structure with `src/` Layout
- Previously scattered scripts across root-level folders
- Now everything lives under `src/stock_prediction_ml/` — cleaner imports, easier packaging
- Following Python packaging best practices from the start

### 3. Separating Dev vs Prod Environments
- Learned this pattern at my previous job, now applied it here
- Separate configs (`local.yaml` vs `prod.yaml`) prevent accidental production issues
- Makes deployment smoother when environments are explicitly defined

### 4. Organizing Data by Purpose
- Instead of dumping everything into a single `data/` folder
- Now structured: `raw/`, `processed/`, `feature/`, `meta/`, `feast/`
- Each folder has a clear responsibility — easier to debug and maintain

### 5. CLI Argument Parsers for Every Script
- Added `argparse` to all runnable scripts (training, ingestion, etc.)
- Enables calling scripts from terminal: `python train.py --config configs/prod.yaml`
- Essential for Docker containers and Kubernetes jobs — no hardcoded paths

### 6. Feature Engineering as a Production Step
- Before: just used raw columns from the dataset
- Now: dedicated script creates 30+ features (RSI, MACD, rolling stats, lags)
- Features are versioned and reproducible — not ad-hoc notebook transformations

### 7. Feast Feature Store Fundamentals
- Completely new topic I learned from scratch
- Key insight: batch features (training) != real-time features (serving) causes skew
- Feast solves this by materializing features to an online store for consistent serving

### 8. API Lifecycle with Health Checks
- Implemented FastAPI `lifespan` function for startup/shutdown logic
- Health check endpoint validates model and Feast store are loaded
- Production APIs need explicit dependency validation — fail fast on startup

### 9. Unit testing for Streamlit UI
- Write test cases for Streamlit pages and functions
- Write class-based test cases.

### 10. AI usage to speed up project
- Learned how to use AI as a pair programmer which helps learning and not vibe coding until everything is broken.
- Before: 1 feature could take me weeks to complete.
- Now: 1-2 days.
- Additionally, I don't want AI to just suggest me the code. I want it to help me learn along the way by asking always to give me skeleton, then I would try to complete it, debug and run before asking for the full code.

---

## Setup & Running Locally

> **Work in Progress** — Full Docker Compose setup coming soon for one-command startup.

### Prerequisites
- Python 3.13+
- [uv](https://github.com/astral-sh/uv) (recommended) or pip
- MarketStack API key (for data ingestion)

### Quick Start
```bash
# Clone and setup
git clone https://github.com/raydiwill/stock-prediction-ml.git
cd stock-prediction-ml
uv sync

# Run tests
uv run pytest -v -m "not slow"

# Apply Feast feature store
cd src/stock_prediction_ml/feast_repo && feast apply

# Start API server (requires trained model + materialized features)
uvicorn src.stock_prediction_ml.api.main:app --reload
```

### Full Pipeline (Manual)
```bash
# 1. Pull stock data
uv run python -m stock_prediction_ml.marketstack.pull

# 2. Validate data
uv run python -m stock_prediction_ml.data_validation.validation

# 3. Ingest raw data into DB
uv run python -m stock_prediction_ml.db.setup_db  # To create db tables if not done
uv run python -m stock_prediction_ml.db.ingest  # Ingestion

# 3. Build features
uv run python -m stock_prediction_ml.features.build_features

# 4. Apply Feast & materialize
cd src/stock_prediction_ml/feast_repo
feast apply
feast materialize-incremental $(date +%Y-%m-%dT%H:%M:%S) # Daily run for only new data
feast materialize 2020-01-01T00:00:00 $(date +%Y-%m-%dT%H:%M:%S) # From beginning of data to now


# 5. Train model
uv run python src/stock_prediction_ml/model/train.py --config configs/training/local.yaml
```

> **Coming Soon**: `docker-compose up` to spin up the entire stack

---

## Project Structure

```
src/stock_prediction_ml/
├── api/             # FastAPI endpoints + middleware
├── config/          # Pydantic settings
├── feast_repo/      # Feature store definitions
├── model/           # Training pipeline + MLflow registry
├── features/        # Feature engineering (30+ indicators)
├── data_validation/ # Great Expectations suite
├── db/              # SQLAlchemy models
└── marketstack/     # Data ingestion

tests/               # Hermetic tests with synthetic fixtures
configs/             # YAML training configurations
notebooks/           # Exploration & tuning (Optuna)
```

---

## Acknowledgments

Built as a learning project to understand production ML systems. Inspired by:
- [Feast documentation](https://docs.feast.dev/)
- [MLflow Model Registry](https://mlflow.org/docs/latest/model-registry.html)

---

*Questions or feedback? Open an issue or reach out!*
