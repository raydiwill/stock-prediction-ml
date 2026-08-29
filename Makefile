COMPOSE_DEV := docker compose -f docker-compose.dev.yml --env-file configs/config.env.dev
TRAIN_START_DATE ?= $(shell date -v-6m +%Y-%m-%d 2>/dev/null || date -d "-6 months" +%Y-%m-%d)
API_CONTAINER_DEV := stock-prediction-fastapi-dev
API_CONTAINER_STAGING := stock-prediction-fastapi-staging
API_CONTAINER_PROD := stock-prediction-fastapi-prod

COMPOSE_STAGING := docker compose -f docker-compose.staging.yml --env-file configs/config.env.staging
COMPOSE_PROD := docker compose -f docker-compose.prod.yml --env-file configs/config.env.prod

.PHONY: up-dev down-dev init logs shell airflow-up airflow-down airflow-password up-prod down-prod init-prod up-staging down-staging init-staging grafana-password-dev grafana-password-staging grafana-password-prod

up-dev:
	$(COMPOSE_DEV) up -d --build --wait

down-dev:
	$(COMPOSE_DEV) down

up-staging:
	@echo "==> Starting staging environment"
	$(COMPOSE_STAGING) up -d --build --wait
	@echo "==> Staging environment is running"

down-staging:
	@echo "==> Stopping staging environment"
	$(COMPOSE_STAGING) down

init-staging:
	@echo "==> feast apply"
	$(COMPOSE_STAGING) exec api sh -c "cd src/stock_prediction_ml/feast_repo && feast apply"
	@echo "==> Training model"
	$(COMPOSE_STAGING) exec api python -m stock_prediction_ml.model.train \
		--config configs/training/staging.yaml \
		--start-date $(TRAIN_START_DATE)
	@echo "==> Promoting latest model to champion"
	$(COMPOSE_STAGING) exec api python -m stock_prediction_ml.model.promote \
		--config configs/training/staging.yaml \
		--force --alias champion
	@echo "==> feast materialize (full range from training start)"
	$(COMPOSE_STAGING) exec api sh -c "feast -c src/stock_prediction_ml/feast_repo materialize $(TRAIN_START_DATE)T00:00:00 $$(date +%Y-%m-%dT%H:%M:%S)"
	@echo "==> Restarting API to pick up trained model..."
	$(COMPOSE_STAGING) restart api
	@echo "==> Waiting for API to be healthy..."
	@until [ "$$(docker inspect --format='{{.State.Health.Status}}' $(API_CONTAINER_STAGING))" = "healthy" ]; do \
		printf '.'; sleep 3; \
	done
	@echo ""
	@echo "==> Done. API is ready to serve predictions."

up-prod:
	@echo "==> Starting production environment"
	$(COMPOSE_PROD) up -d --build --wait
	@echo "==> Production environment is running"

down-prod:
	@echo "==> Stopping production environment"
	$(COMPOSE_PROD) down

init-prod:
	@echo "==> feast apply"
	$(COMPOSE_PROD) exec api sh -c "cd src/stock_prediction_ml/feast_repo && feast apply"
	@echo "==> Training model"
	$(COMPOSE_PROD) exec api python -m stock_prediction_ml.model.train \
		--config configs/training/prod.yaml \
		--start-date $(TRAIN_START_DATE)
	@echo "==> Promoting latest model to champion"
	$(COMPOSE_PROD) exec api python -m stock_prediction_ml.model.promote \
		--config configs/training/prod.yaml \
		--force --alias champion
	@echo "==> feast materialize (full range from training start)"
	$(COMPOSE_PROD) exec api sh -c "feast -c src/stock_prediction_ml/feast_repo materialize $(TRAIN_START_DATE)T00:00:00 $$(date +%Y-%m-%dT%H:%M:%S)"
	@echo "==> Restarting API to pick up trained model..."
	$(COMPOSE_PROD) restart api
	@echo "==> Waiting for API to be healthy..."
	@until [ "$$(docker inspect --format='{{.State.Health.Status}}' $(API_CONTAINER_PROD))" = "healthy" ]; do \
		printf '.'; sleep 3; \
	done
	@echo ""
	@echo "==> Done. API is ready to serve predictions."

init:
	@echo "==> feast apply"
	$(COMPOSE_DEV) exec api sh -c "cd src/stock_prediction_ml/feast_repo && feast apply"
	@echo "==> Training model"
	$(COMPOSE_DEV) exec api python -m stock_prediction_ml.model.train \
		--config configs/training/dev.yaml \
		--start-date $(TRAIN_START_DATE)
	@echo "==> Promoting latest model to champion"
	$(COMPOSE_DEV) exec api python -m stock_prediction_ml.model.promote \
		--config configs/training/dev.yaml \
		--force --alias champion
	@echo "==> feast materialize (full range from training start)"
	$(COMPOSE_DEV) exec api sh -c "feast -c src/stock_prediction_ml/feast_repo materialize $(TRAIN_START_DATE)T00:00:00 $$(date +%Y-%m-%dT%H:%M:%S)"
	@echo "==> Restarting API to pick up trained model..."
	$(COMPOSE_DEV) restart api
	@echo "==> Waiting for API to be healthy..."
	@until [ "$$(docker inspect --format='{{.State.Health.Status}}' $(API_CONTAINER_DEV))" = "healthy" ]; do \
		printf '.'; sleep 3; \
	done
	@echo ""
	@echo "==> Done. API is ready to serve predictions."

logs:
	$(COMPOSE_DEV) logs -f api

shell:
	$(COMPOSE_DEV) exec api bash

airflow-up:
	@echo "==> Init airflow"
	$(COMPOSE_DEV) --profile airflow up -d --build --wait
	@echo "==> Airflow UI: http://localhost:8080"
	@$(MAKE) airflow-password

airflow-down:
	$(COMPOSE_DEV) stop airflow airflow-init
	$(COMPOSE_DEV) rm -f airflow-init

airflow-password:
	@grep AIRFLOW_ADMIN configs/config.env.dev

grafana-password-dev:
	@grep GRAFANA_ADMIN configs/config.env.dev

grafana-password-staging:
	@grep GRAFANA_ADMIN configs/config.env.staging

grafana-password-prod:
	@grep GRAFANA_ADMIN configs/config.env.prod