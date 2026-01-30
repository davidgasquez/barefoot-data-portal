lint:
	uv run ruff check
	uv run ty check

run:
	uv run bdp materialize --all
