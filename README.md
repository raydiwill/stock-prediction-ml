# Stock Price Prediction ML Pipeline

> End-to-end machine learning system for predicting next-day stock price direction using production-grade MLOps practices.

![Python](https://img.shields.io/badge/Python-3.13-blue)
![MLflow](https://img.shields.io/badge/MLflow-Model%20Registry-orange)
![Feast](https://img.shields.io/badge/Feast-Feature%20Store-green)
![FastAPI](https://img.shields.io/badge/FastAPI-REST%20API-teal)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-red)
![Airflow](https://img.shields.io/badge/Airflow-Orchestration-017CEE)
![Status](https://img.shields.io/badge/Status-95%25%20Complete-yellow)

---

## Status

**Current Stage**: Full ML pipeline with REST API, Streamlit dashboard, Airflow orchestration, and comprehensive test suite.

**Next Step**: Monitoring (Grafana/Prometheus) and Docker containerization.

| Component | Status |
|-----------|--------|
| Data Ingestion | ✅ Complete |
| Data Validation | ✅ Complete |
| Feature Engineering | ✅ Complete |
| Feature Store (Feast) | ✅ Complete |
| Model Training + Registry | ✅ Complete |
| REST API | ✅ Complete |
| Streamlit UI | ✅ Complete |
| Testing (80+ tests) | ✅ Complete |
| Airflow Orchestration | ✅ Complete |
| Monitoring (Grafana) | ⏳ Planned |
| Docker Deployment | ⏳ Planned |

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
- Orchestrates the full pipeline via **Airflow** DAGs (ingestion, features, training, prediction)
- Visualizes predictions via a **Streamlit** dashboard with interactive charts

**Why this project?** To try and get rich! *(But seriously speaking, I want to apply my uni knowledge and tutorial hells into practical projects, something I can used in daily stock trading)*

---

## Live Demo

> **Coming Soon** — A live demo will be available once the project is containerized and deployed. The Streamlit dashboard currently runs locally with 3 pages: Live Prediction, Historical Data, and About Model.

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

### Orchestration
| Technology | Purpose |
|------------|---------|
| **Airflow** | DAG-based pipeline orchestration (ingestion → features → training → prediction) |

### API & UI
| Technology | Purpose |
|------------|---------|
| **FastAPI** | REST API for model serving with prediction persistence |
| **Streamlit** | Multi-page dashboard (Live Prediction, Historical Data, About Model) |
| **Plotly** | Interactive price charts with prediction overlays |
| **SQLAlchemy** | ORM for prediction audit trail and stock data storage |
| **Pydantic** | Request/response schema validation |
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

### 9. Unit Testing for Streamlit UI
- Tested pure logic (utils, API client, chart builders) rather than Streamlit rendering (low ROI due to side effects)
- Used class-based test organization and httpx mocking for API client tests
- `conftest.py` mocks Streamlit module before imports to prevent server initialization in CI

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

# Start Streamlit UI (requires API server running)
./run_ui.sh
# or: streamlit run src/stock_prediction_ml/ui/app.py
```

### Full Pipeline (Airflow)
```bash
# Set up Airflow
export AIRFLOW_HOME=$(pwd)/orchestration
airflow standalone

# DAGs run on schedule:
# - ingestion_dag:  Daily @ 07:00 UTC (fetch → validate → ingest)
# - feature_engineering_dag: Manual trigger (build features → materialize to Feast)
# - training_dag:   Weekly Sunday @ 09:00 UTC (train → auto-promote)
# - prediction_dag: Daily @ 08:00 UTC (materialize → batch predict)

# Run Airflow DAG tests
pytest orchestration/tests/ -v
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

# 6. Start API + UI
uvicorn src.stock_prediction_ml.api.main:app --reload
./run_ui.sh  # In a separate terminal
```

> **Coming Soon**: `docker-compose up` to spin up the entire stack

---

## Project Structure

```
src/stock_prediction_ml/
├── api/             # FastAPI endpoints + middleware + schema
├── config/          # Centralized Pydantic settings
├── feast_repo/      # Feature store definitions + services
├── model/           # Training pipeline + MLflow registry + pyfunc wrapper
├── features/        # Feature engineering (30+ indicators)
├── data_validation/ # Great Expectations suite
├── db/              # SQLAlchemy models + session + ingestion
├── marketstack/     # Data ingestion from MarketStack API
└── ui/              # Streamlit dashboard
    ├── app.py           # Entry point + navigation
    ├── utils.py         # Symbol validation, formatting
    ├── components/      # API client (httpx), Plotly charts
    └── pages/           # Prediction, Historical, About Model

tests/               # 9 modules, 80+ hermetic tests
configs/             # YAML training configurations
notebooks/           # Exploration & tuning (01-06)

orchestration/
├── dags/            # Airflow DAGs (ingestion, features, training, prediction)
├── tests/           # DAG validation + per-DAG structure tests
└── airflow.cfg      # Airflow config (LocalExecutor, SQLite)
```

---

## Acknowledgments

Built as a learning project to understand production ML systems. Inspired by:
- [Feast documentation](https://docs.feast.dev/)
- [MLflow Model Registry](https://mlflow.org/docs/latest/model-registry.html)
- [Streamlit documentation](https://docs.streamlit.io/)
- [FastAPI documentation](https://fastapi.tiangolo.com/)
- [Apache Airflow documentation](https://airflow.apache.org/docs/)

---

*Questions or feedback? Open an issue or reach out!*
