# CLAUDE.md

> This file provides context for Claude Code. For full project details, see [PROJECT_SUMMARY.md](PROJECT_SUMMARY.md).

## Project Overview

**Stock Price Prediction ML Pipeline** - Predicts next-day stock direction (up/down) using CatBoost classifier with MLflow tracking and Feast feature store.

**Stack**: Python 3.13, FastAPI, MLflow, Feast, CatBoost, pytest, SQLAlchemy

## Quick Commands

```bash
# Run all tests
pytest -v

# Run fast tests only (skip @pytest.mark.slow)
pytest -v -m "not slow"

# Run specific test file
pytest tests/test_api.py -v

# Start API server
uvicorn src.stock_prediction_ml.api.main:app --reload

# Apply Feast feature definitions
cd src/stock_prediction_ml/feast_repo && feast apply

# Materialize features to online store
feast materialize-incremental $(date +%Y-%m-%dT%H:%M:%S)
```

## Current Focus

**Next Tasks**:
- **Orchestration**: Airflow DAGs for automated pipeline execution

## Future implementation

- **Monitoring**: Grafana/Prometheus for drift detection
- **Deployment**: Docker containerization
- **UI**: Streamlit dashboard for predictions

## Code Style

- **Type hints**: Required for all function signatures
- **Docstrings**: Google style for public functions/classes
- **Imports**: stdlib → third-party → local (isort compatible)
- **Line length**: 88 characters (Black formatter)
- **Tests**: Use synthetic fixtures, no external dependencies in CI

## Key Patterns

### MLflow Model Loading
```python
model = mlflow.pyfunc.load_model("models:/stock_prediction_classifier@champion")
predictions = model.predict(df)  # Encoding handled internally
```

### Feast Feature Retrieval
```python
store = FeatureStore(repo_path="src/stock_prediction_ml/feast_repo")
features = store.get_online_features(
    features=store.get_feature_service("stock_prediction_service"),
    entity_rows=[{"symbol": "AAPL"}]
).to_df()
```

### Test Mocking
```python
@pytest.fixture
def mock_model(mocker):
    mock = mocker.MagicMock()
    mock.predict.return_value = pd.DataFrame({...})
    mocker.patch("mlflow.pyfunc.load_model", return_value=mock)
    return mock
```

## Project Structure

```
src/stock_prediction_ml/
├── api/           # FastAPI endpoints (main.py, schema.py)
├── config/        # Settings via Pydantic (settings.py)
├── feast_repo/    # Feature store definitions
├── model/         # Training pipeline (train.py)
├── features/      # Feature engineering
├── db/            # SQLAlchemy models
└── marketstack/   # Data ingestion

tests/             # Pytest test modules
configs/           # YAML configs (training/local.yaml)
notebooks/         # Jupyter exploration (01-06)
```

## Important Constraints

- **No data leakage**: Use temporal splits (date quantiles), never random splits
- **Hermetic tests**: No external API calls or DB writes in CI
- **SQLite int64 issue**: Feast returns int64, cast to int32 for model signature
- **Single feature file**: All Feast views in `features_definition.py` (prevents import duplication)

## Workflow Preference

1. **Skeleton first**: Provide structure with hints, no obvious hints just ideas, let me implement
2. **Debug together**: Help fix issues when I run into errors
3. **Then refactor**: Improve to production standards after it works
4. **Explain why**: Always explain the reasoning behind suggestions

## Files to Reference

| Purpose | File |
|---------|------|
| Full project context | [PROJECT_SUMMARY.md](PROJECT_SUMMARY.md) |
| API implementation | [src/stock_prediction_ml/api/main.py](src/stock_prediction_ml/api/main.py) |
| Model training | [src/stock_prediction_ml/model/train.py](src/stock_prediction_ml/model/train.py) |
| Settings | [src/stock_prediction_ml/config/settings.py](src/stock_prediction_ml/config/settings.py) |
| Test patterns | [tests/test_train.py](tests/test_train.py), [tests/test_api.py](tests/test_api.py) |
| Training config | [configs/training/local.yaml](configs/training/local.yaml) |
