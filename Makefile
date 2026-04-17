COMPOSE := docker compose -f docker-compose.dev.yml
TRAIN_START_DATE ?= $(shell date -v-6m +%Y-%m-%d 2>/dev/null || date -d "-6 months" +%Y-%m-%d)
API_CONTAINER := stock-prediction-fastapi

.PHONY: up down init logs shell airflow-up airflow-down airflow-password

up:
	$(COMPOSE) up -d --build --wait

down:
	$(COMPOSE) down

init:
	@echo "==> feast apply"
	$(COMPOSE) exec api sh -c "cd src/stock_prediction_ml/feast_repo && feast apply"
	@echo "==> Training model"
	$(COMPOSE) exec api python -m stock_prediction_ml.model.train \
		--config configs/training/local.yaml \
		--start-date $(TRAIN_START_DATE)
	@echo "==> Promoting latest model to champion"
	$(COMPOSE) exec api python -m stock_prediction_ml.model.promote \
		--config configs/training/local.yaml \
		--force --alias champion
	@echo "==> feast materialize (full range from training start)"
	$(COMPOSE) exec api sh -c "feast -c src/stock_prediction_ml/feast_repo materialize $(TRAIN_START_DATE)T00:00:00 $$(date +%Y-%m-%dT%H:%M:%S)"
	@echo "==> Restarting API to pick up trained model..."
	$(COMPOSE) restart api
	@echo "==> Waiting for API to be healthy..."
	@until [ "$$(docker inspect --format='{{.State.Health.Status}}' $(API_CONTAINER))" = "healthy" ]; do \
		printf '.'; sleep 3; \
	done
	@echo ""
	@echo "==> Done. API is ready to serve predictions."

logs:
	$(COMPOSE) logs -f api

shell:
	$(COMPOSE) exec api bash

airflow-up:
	@echo "==> Init airflow"
	$(COMPOSE) --profile airflow up -d --build --wait
	@echo "==> Airflow UI: http://localhost:8080"
	@until docker logs stock-prediction-ml-airflow-1 2>&1 | grep -q "Password for user"; do sleep 2; done
	@docker logs stock-prediction-ml-airflow-1 2>&1 | grep "Password for user" || true

airflow-down:
	$(COMPOSE) stop airflow airflow-init
	$(COMPOSE) rm -f airflow-init

airflow-password:
	@docker logs stock-prediction-ml-airflow-1 2>&1 | grep "Password for user" || true
