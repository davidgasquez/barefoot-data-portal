# Repository Guidelines

The Barefoot Data Portal is a minimalistic and functional open data platform to help get, transform and publish datasets in the age of agents.

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

## Opinions

- Each asset is responsible for its own materialization and dependencies
- Datasets are files without any glue code
- Use full refresh pipelines as default

## Python

- Always use `uv`
  - `uv run file.py`
  - `uv add`
  - `uv --help`
