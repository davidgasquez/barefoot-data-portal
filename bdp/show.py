from pathlib import Path

import duckdb

from bdp.api import db_connection, find_assets_root
from bdp.materialize import Asset, discover_assets


def show_asset(name: str, sample_rows: int = 5) -> None:
    if sample_rows < 1:
        raise ValueError("sample_rows must be at least 1")

    assets_root = find_assets_root()
    assets = discover_assets(assets_root)
    try:
        asset = assets[name]
    except KeyError as exc:
        raise ValueError(f"Unknown asset: {name}") from exc

    project_root = assets_root.parent
    print(f"asset: {asset.key}")
    print(f"path: {asset.path.relative_to(project_root).as_posix()}")
    print(f"kind: {asset.kind}")
    print(f"resolved key: {asset.key}")
    print()
    print("depends:")
    if asset.depends:
        for dependency in asset.depends:
            print(f"  - {dependency}")
    else:
        print("  - none")
    print()
    print("tests:")
    test_lines = asset_test_lines(asset, project_root)
    if test_lines:
        for line in test_lines:
            print(f"  - {line}")
    else:
        print("  - none")
    print()
    print("sample:")
    with db_connection() as conn:
        if not table_exists(conn, asset):
            print("  not materialized")
            return
        columns, rows = sample_table(conn, asset, sample_rows)
    for line in render_sample(columns, rows):
        print(f"  {line}")


def asset_test_lines(asset: Asset, project_root: Path) -> list[str]:
    lines = [
        *(f"not_null: {column}" for column in asset.tests.not_null),
        *(f"unique: {column}" for column in asset.tests.unique),
        *(f"assert: {assertion}" for assertion in asset.tests.assertions),
    ]
    for path in custom_test_paths(project_root, asset.key):
        lines.append(f"custom: {path.relative_to(project_root).as_posix()}")
    return lines


def custom_test_paths(project_root: Path, asset_key: str) -> list[Path]:
    tests_root = project_root / "tests" / "data"
    if not tests_root.is_dir():
        return []
    return sorted(tests_root.rglob(f"{asset_key}__*.test.sql"))


def table_exists(conn: duckdb.DuckDBPyConnection, asset: Asset) -> bool:
    row = conn.execute(
        "select 1 from information_schema.tables "
        "where table_schema = ? and table_name = ? limit 1",
        [asset.schema, asset.name],
    ).fetchone()
    return row is not None


def sample_table(
    conn: duckdb.DuckDBPyConnection,
    asset: Asset,
    limit: int,
) -> tuple[list[str], list[tuple[object, ...]]]:
    cursor = conn.execute(f"select * from {asset.key} limit {limit}")
    rows = cursor.fetchall()
    columns = [description[0] for description in cursor.description]
    return columns, rows


def render_sample(columns: list[str], rows: list[tuple[object, ...]]) -> list[str]:
    if not columns:
        return ["no columns"]
    if not rows:
        return ["no rows"]

    rendered_rows = [[format_value(value) for value in row] for row in rows]
    widths = [len(column) for column in columns]
    for row in rendered_rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))

    header = " | ".join(
        column.ljust(widths[index]) for index, column in enumerate(columns)
    )
    divider = "-+-".join("-" * width for width in widths)
    body = [
        " | ".join(value.ljust(widths[index]) for index, value in enumerate(row))
        for row in rendered_rows
    ]
    return [header, divider, *body]


def format_value(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)
