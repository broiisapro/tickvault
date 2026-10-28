.PHONY: sync lint types test check bench requirements docker-build demo-live demo-buffer demo-down

sync:
	uv sync --frozen

lint:
	uv run ruff check .

types:
	uv run mypy tickvault

test:
	uv run pytest -q

check: lint types test

bench:
	uv run tickvault bench

requirements:
	uv export --frozen --no-default-groups --group dashboard --no-hashes --no-header -o dashboard/requirements.txt

docker-build:
	docker compose build

demo-live:
	docker compose up -d --build
	@echo "TickVault dashboard: http://localhost:8501"

demo-buffer:
	TICKVAULT_BUFFER=redpanda:9092 docker compose --profile buffer --profile metrics up -d --build
	@echo "TickVault dashboard:  http://localhost:8501"
	@echo "TickVault Grafana:    http://localhost:3001 (anonymous viewer; admin password from TICKVAULT_GRAFANA_ADMIN_PASSWORD, default tickvault-admin)"
	@echo "TickVault Prometheus: http://localhost:9090"
	@echo "Ingester metrics:     http://localhost:9108/metrics"
	@echo "Writer metrics:       http://localhost:9109/metrics"

demo-down:
	docker compose --profile buffer --profile metrics --profile orchestration down
