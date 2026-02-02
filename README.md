# Barefoot Data Platform

Minimal, local first data platform. Assets are files in `datasets/` that materialize into DuckDB.

## Quickstart

- `uv run bdp list`
- `uv run bdp materialize --all`
- `uv run bdp materialize raw.base_numbers`

DuckDB lives at `bdp.duckdb` in the current working directory. Override with `BDP_DB_PATH`.

## Datasets

- Assets live in `datasets/` and its subdirectories.
- Supported suffixes are `.py`, `.sql`, and `.sh`.
- Files starting with `_` are ignored.
- The runner searches for the nearest `datasets/` directory from the current working directory.

## Metadata

Metadata is a leading comment block at the top of each asset file.

Python and bash use `#`.

```python
# dataset.name = base_numbers
# dataset.schema = raw
# dataset.depends = raw.other_table, raw.more_tables
```

SQL uses `--`.

```sql
-- dataset.name = base_numbers
-- dataset.schema = raw
```

Fields:

- `dataset.name` required
- `dataset.schema` required
- `dataset.depends` optional comma separated list of `schema.table`

## Asset types

### Python

Define a function named after `dataset.name`. It must return a `polars.DataFrame`.

```python
# dataset.name = base_numbers
# dataset.schema = raw
import polars as pl

def base_numbers() -> pl.DataFrame:
    return pl.DataFrame({"value": [1, 2, 3]})
```

### SQL

The file is a SQL query. The runner executes it as:

```sql
create or replace table schema.table as <sql>
```

### Bash

The script must create the table using environment variables.

- `BDP_DB_PATH` path to DuckDB
- `BDP_SCHEMA` target schema
- `BDP_TABLE` target table

```bash
#!/usr/bin/env bash
# dataset.name = cli_numbers
# dataset.schema = raw

set -euo pipefail

duckdb "${BDP_DB_PATH}" <<SQL
create or replace table ${BDP_SCHEMA}.${BDP_TABLE} as
select 1 as value
SQL
```

## CLI

- `bdp list`
- `bdp materialize --all`
- `bdp materialize schema.table [schema.table ...]`

## Python API

```python
import bdp

bdp.sql("create or replace table raw.example as select 1 as value")
frame = bdp.table("raw.example")
```

Utilities:

- `bdp.table` returns a `polars.DataFrame`
- `bdp.sql` and `bdp.query` execute SQL
- `bdp.db_connection` gives a DuckDB connection
