# Builder stage
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

WORKDIR /usr/local/app

RUN apt-get update && apt-get install -y --no-install-recommends gcc python3-dev && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# Runtime stage
# API
FROM python:3.13-slim AS api 

WORKDIR /usr/local/app

COPY --from=builder /usr/local/app/.venv ./.venv

ENV PATH="/usr/local/app/.venv/bin:$PATH"

RUN useradd --create-home --no-log-init appuser && chown appuser:appuser /usr/local/app

COPY --chown=appuser:appuser src/ ./src/
COPY --chown=appuser:appuser configs/ ./configs/

USER appuser

RUN mkdir -p docs/images

EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

CMD ["uvicorn", "stock_prediction_ml.api.main:app", "--host", "0.0.0.0", "--port", "8000"]

# UI
FROM python:3.13-slim AS ui 

WORKDIR /usr/local/app 

COPY --from=builder /usr/local/app/.venv ./.venv

ENV PATH="/usr/local/app/.venv/bin:$PATH"

RUN useradd --create-home --no-log-init appuser && chown appuser:appuser /usr/local/app

COPY --chown=appuser:appuser src/ ./src/
COPY --chown=appuser:appuser configs/ ./configs/

USER appuser

EXPOSE 8501
CMD ["streamlit", "run", "src/stock_prediction_ml/ui/app.py", "--server.port=8501", "--server.address=0.0.0.0"]

# Airflow worker
FROM python:3.13-slim AS worker

WORKDIR /usr/local/app

COPY --from=builder /usr/local/app/.venv ./.venv

ENV PATH="/usr/local/app/.venv/bin:$PATH"

RUN useradd --create-home --no-log-init airflow-worker && chown airflow-worker:airflow-worker /usr/local/app

COPY --chown=airflow-worker:airflow-worker src/ ./src/
COPY --chown=airflow-worker:airflow-worker configs/ ./configs/

USER airflow-worker

# Airflow
FROM apache/airflow:3.2.0-python3.13 AS airflow

WORKDIR /usr/local/app

USER root
COPY --from=builder /usr/local/app/.venv ./.venv

RUN chown -R airflow:root .venv
USER airflow

ENV PATH="/usr/local/app/.venv/bin:$PATH"

COPY --chown=airflow:airflow src/ ./src/
COPY --chown=airflow:airflow configs/ ./configs/
COPY --chown=airflow:airflow orchestration/ ./orchestration/
