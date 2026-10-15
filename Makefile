.PHONY: sync lint types test check

sync:
	uv sync --frozen

lint:
	uv run ruff check .

types:
	uv run mypy tickvault

test:
	uv run pytest -q

check: lint types test
