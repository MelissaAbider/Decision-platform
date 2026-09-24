.PHONY: install lint test run hooks

install:
	uv sync --frozen --extra dev

lint:
	uv run --frozen ruff check .
	uv run --frozen ruff format --check .

test:
	uv run --frozen pytest

run:
	uv run --frozen python -m ai_decision_platform

hooks:
	uv run --frozen pre-commit install
