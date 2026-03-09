# Repository Guidelines

The Barefoot Data Platform is a minimalistic and functional open data platform to help get, transform and publish datasets in the age of agents.

## Principles

- Minimal, simple/UNIXy, and opinionated
- Functional and idempotent transformations/pipelines
- Modular, declarative, independent, composable steps
- Low abstractions, no frameworks
- Everything is text/code, everything is versioned
- Colocated assets, metadata, tests, and documentation
- Quick feedback cycles
  - Run any asset locally and immediately see results
  - Easy to debug
- No backward compatibility constraints
- Assets can fail without taking down the whole run

## Opinions

- Assets are files
- Stateless runs

## Code

- Run checks with `make check` after writing assets
- Always use `uv`
  - `uv run file.py`
  - `uv add`
  - `uv --help`

## Writing Assets

- Write assets (`.py` and `.sql`) inside the `assets/` folder
  - First level directory is the database schema. Further folders become prefixes.
    - `assets/raw/base_numbers.py` becomes the table `raw.base_numbers`
    - `assets/raw/alt/base_numbers.py` becomes the table `raw.alt_base_numbers`
- Metadata block at file top as language comments
  - Optional `asset.description`
  - Optional and repeatable `asset.depends`

### Python Assets

- Define a callable function that returns a `polars.DataFrame` or `None`(custom materialization).
- Use `bdp.table("schema.table")` to load a dependency
- Use `bdp.sql("sql query")` to run arbitrary SQL against the database

### SQL Assets

- File content is a SQL query only
- Runner executes `create or replace table schema.file_stem as <sql>`
