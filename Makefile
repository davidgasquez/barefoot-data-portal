.DEFAULT_GOAL := run

lint:
	uv run ruff check
	uv run ty check

test:
	uv run pytest
	uv run bdp test

data-test:
	uv run bdp test

check: lint
	uv run bdp check

run:
	uv run bdp materialize
