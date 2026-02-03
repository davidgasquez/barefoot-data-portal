from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb
import polars as pl


def get_db_path(db_path: Path | str | None = None) -> Path:
    if db_path is None:
        return Path(os.environ.get("BDP_DB_PATH", "bdp.duckdb"))
    return Path(db_path)


@contextmanager
def db_connection(
    db_path: Path | str | None = None,
) -> Iterator[duckdb.DuckDBPyConnection]:
    path = get_db_path(db_path)
    with duckdb.connect(path) as conn:
        yield conn


def sql(
    statement: str,
    params: list[object] | None = None,
    *,
    db_path: Path | str | None = None,
) -> None:
    cleaned = statement.strip()
    with db_connection(db_path) as conn:
        if params is None:
            conn.execute(cleaned)
            return
        conn.execute(cleaned, params)


def table(name: str, *, db_path: Path | str | None = None) -> pl.DataFrame:
    with db_connection(db_path) as conn:
        arrow_table = conn.execute(f"select * from {name}").fetch_arrow_table()
    return pl.DataFrame(arrow_table)


def find_assets_root() -> Path:
    for parent in [Path.cwd(), *Path.cwd().parents]:
        candidate = parent / "assets"
        if candidate.is_dir():
            return candidate
    raise FileNotFoundError("assets directory not found")
