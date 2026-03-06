# Repository Guidelines

The Barefoot Data Platform is a minimalistic and functional open data platform
to help get, transform and publish datasets in the age of agents.

## Principles

- Minimal, simple/UNIXy, and opinionated
- Functional and idempotent transformations/pipelines
- Modular, declarative, independent, composable steps
- Low abstractions, no frameworks
- Everything is text/code, everything is versioned
- Colocated metadata and documentation
- Quick feedback cycles
  - Run any asset locally and immediately see results
  - Easy to debug
- No backward compatibility constraints
- Assets can fail without taking down the whole run

## Opinions

- Each asset is responsible for its own materialization and dependencies
- Datasets are files without any glue code
- Use full refresh pipelines as default

## Code

- Always `make run` after changing code
- Check `README.md` is up to date
- Always use `uv`
  - `uv run file.py`
  - `uv add`
  - `uv --help`

## Writing Assets

- Write assets (`.py` and `.sql`) inside the `assets/` folder and subdirectories
- File name is the table name
- Metadata block at file top as language comments
  - Required `asset.schema`
  - Optional `asset.description`
  - Optional `asset.depends` (can be repeated)
- `asset.name` is not supported
- Run checks with `uv run bdp check` after writing assets

### Python Assets

- Define a callable function named after the file stem with no arguments
- Return a `polars.DataFrame`
- Use `bdp.table("schema.table")` to read dependencies
- Use `bdp.sql("sql query")` to run arbitrary SQL against the database

### SQL Assets

- File content is a SQL query only
- Runner executes `create or replace table schema.file_stem as <sql>`
