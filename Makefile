.PHONY: sync lint types test check docker-build demo-live demo-down

sync:
	uv sync --frozen

lint:
	uv run ruff check .

types:
	uv run mypy tickvault

test:
	uv run pytest -q

check: lint types test

docker-build:
	docker compose build

demo-live:
	docker compose up -d --build
	@echo "TickVault dashboard: http://localhost:8501"

demo-down:
	docker compose down
