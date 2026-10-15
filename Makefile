.PHONY: sync lint types test check bench requirements docker-build demo-live demo-down

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

demo-down:
	docker compose down
