# Barefoot Data Platform

Minimal, local first data platform. Assets are files in `assets/` that materialize into DuckDB.

## Quickstart

- `uv run bdp list`
- `uv run bdp check`
- `uv run bdp materialize`
- `uv run bdp materialize raw.base_numbers`

DuckDB lives at `bdp.duckdb` in the current working directory. Override with `BDP_DB_PATH`.

## Assets

- Assets live in `assets/` and its subdirectories.
- Supported suffixes are `.py`, `.sql`, and `.sh`.
- Files starting with `_` are ignored.
- The runner searches for the nearest `assets/` directory from the current working directory.

## Metadata

Metadata is a leading comment block at the top of each asset file. Lines starting with `asset.` are parsed.

Python and bash use `#`.

```python
# asset.name = base_numbers
# asset.schema = raw
# asset.depends = raw.other_table, raw.more_tables
```

SQL uses `--`.

```sql
-- asset.name = base_numbers
-- asset.schema = raw
```

Fields:

- `asset.name` required (table name)
- `asset.schema` required
- `asset.depends` optional comma separated list of `schema.table` (repeatable)
- other comment lines in the block are ignored

## Asset types

### Python

Define a function named after `asset.name`. It must return a `polars.DataFrame`.

```python
# asset.name = base_numbers
# asset.schema = raw
import polars as pl

def base_numbers() -> pl.DataFrame:
    return pl.DataFrame({"value": [1, 2, 3]})
```

### SQL

The file is a SQL query with metadata.

```sql
-- asset.name = base_numbers
-- asset.schema = raw

select 1 as value
```

The runner executes it as:

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
# asset.name = cli_numbers
# asset.schema = raw

set -euo pipefail

duckdb "${BDP_DB_PATH}" <<SQL
create or replace table ${BDP_SCHEMA}.${BDP_TABLE} as
select 1 as value
SQL
```

## CLI

- `bdp list`
- `bdp check`
- `bdp materialize`
- `bdp materialize schema.table [schema.table ...]`

## Python API

```python
import bdp

bdp.sql("create or replace table raw.example as select 1 as value")
frame = bdp.table("raw.example")
```

Utilities:

- `bdp.table` returns a `polars.DataFrame`
- `bdp.sql` executes SQL
- `bdp.db_connection` gives a DuckDB connection
