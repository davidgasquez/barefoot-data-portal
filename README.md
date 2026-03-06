# Barefoot Data Platform

Minimal, local-first data platform. Assets are Python or SQL files in `assets/`
that materialize into DuckDB.

## Quickstart

- `uv run bdp list`
- `uv run bdp check`
- `uv run bdp materialize`
- `uv run bdp materialize raw.base_numbers`

DuckDB lives at `bdp.duckdb` in the current working directory. Override with
`BDP_DB_PATH`.

## Assets

- Assets live in `assets/` and its subdirectories.
- Supported suffixes are `.py` and `.sql`.
- The file name is the table name.
- The asset key is `schema.file_stem`.
- Files starting with `_` are ignored.
- The runner searches for the nearest `assets/` directory from the current
  working directory.

## Metadata

Metadata is a leading comment block at the top of each asset file.

- Python uses `#`
- SQL uses `--`

Supported keys:

- `asset.schema` required
- `asset.description` optional free text stored as a table comment
- `asset.depends` optional comma-separated `schema.table` references and may be
  repeated

`asset.name` is not supported. Table names come from the file name.

Other comment lines in the metadata block are ignored.

## Asset Types

### Python

Define a function named after the file name. It must return a
`polars.DataFrame`.

```python
# asset.schema = raw
# asset.description = Base numbers for demos
import polars as pl


def base_numbers() -> pl.DataFrame:
    return pl.DataFrame({"value": [1, 2, 3]})
```

Python assets can use the public API to read dependencies or run SQL.

### SQL

The file is a SQL query with metadata.

```sql
-- asset.schema = raw
-- asset.description = Base numbers for demos

select 1 as value
```

The runner executes it as:

```sql
create or replace table schema.file_stem as <sql>
```

## CLI

- `bdp list`
- `bdp check`
- `bdp materialize`
- `bdp materialize schema.table [schema.table ...]`
- `bdp docs --out index.html`

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
