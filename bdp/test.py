from dataclasses import dataclass
from pathlib import Path

import duckdb

from bdp.api import db_connection, find_assets_root
from bdp.materialize import (
    Asset,
    materialize_assets,
    ordered_assets,
    validate_asset_reference,
)


@dataclass(frozen=True)
class DataTest:
    name: str
    query: str
    source: str


def test_assets(sample_rows: int = 10) -> None:
    if sample_rows < 1:
        raise ValueError("sample_rows must be at least 1")

    assets = ordered_assets()
    materialize_assets(assets)

    tests = collect_data_tests(assets)
    if not tests:
        print("No data tests found.", flush=True)
        return

    run_data_tests(tests, sample_rows)


def collect_data_tests(assets: list[Asset]) -> list[DataTest]:
    project_root = find_assets_root().parent
    indexed_assets = {asset.key: asset for asset in assets}
    return [
        *inline_data_tests(assets, project_root),
        *custom_sql_tests(project_root, indexed_assets),
    ]


def inline_data_tests(assets: list[Asset], project_root: Path) -> list[DataTest]:
    tests: list[DataTest] = []
    for asset in assets:
        source = f"metadata:{asset.path.relative_to(project_root).as_posix()}"
        if asset.tests.not_null:
            tests.append(
                DataTest(
                    name=f"{asset.key}__not_null",
                    query=not_null_query(asset),
                    source=source,
                )
            )

        for column in asset.tests.unique:
            tests.append(
                DataTest(
                    name=f"{asset.key}__unique_{column}",
                    query=unique_query(asset, column),
                    source=source,
                )
            )

        for index, assertion in enumerate(asset.tests.assertions, start=1):
            tests.append(
                DataTest(
                    name=f"{asset.key}__assert_{index}",
                    query=assertion_query(asset, assertion),
                    source=source,
                )
            )
    return tests


def custom_sql_tests(
    project_root: Path,
    assets: dict[str, Asset],
) -> list[DataTest]:
    tests_root = project_root / "tests" / "data"
    if not tests_root.is_dir():
        return []
    return [
        sql_data_test_from_path(path, project_root, assets)
        for path in sorted(tests_root.rglob("*.test.sql"))
    ]


def sql_data_test_from_path(
    path: Path,
    project_root: Path,
    assets: dict[str, Asset],
) -> DataTest:
    base_name = path.name.removesuffix(".test.sql")
    asset_key, separator, test_name = base_name.partition("__")
    if not separator or not test_name:
        raise ValueError(
            f"Invalid data test file name '{path}'. "
            "Expected schema.table__name.test.sql"
        )

    validate_asset_reference(asset_key, path)
    if asset_key not in assets:
        raise ValueError(f"Unknown asset '{asset_key}' referenced in {path}")

    return DataTest(
        name=f"{asset_key}__{test_name}",
        query=read_test_query(path),
        source=path.relative_to(project_root).as_posix(),
    )


def read_test_query(path: Path) -> str:
    query = path.read_text(encoding="utf-8").strip()
    if not query:
        raise ValueError(f"Data test file is empty: {path}")
    if query.endswith(";"):
        return query[:-1].rstrip()
    return query


def not_null_query(asset: Asset) -> str:
    conditions = " or ".join(f"{column} is null" for column in asset.tests.not_null)
    return f"select * from {asset.key} where {conditions}"


def unique_query(asset: Asset, column: str) -> str:
    return (
        f"select {column}, count(*) as n "
        f"from {asset.key} "
        f"group by {column} "
        "having count(*) > 1"
    )


def assertion_query(asset: Asset, assertion: str) -> str:
    return f"select * from {asset.key} where not ({assertion})"


def run_data_tests(tests: list[DataTest], sample_rows: int) -> None:
    total = len(tests)
    count_width = len(str(total))
    name_width = max((len(test.name) for test in tests), default=0)
    failed_tests: list[str] = []

    with db_connection() as conn:
        for index, test in enumerate(tests, start=1):
            failing_rows = count_failing_rows(conn, test.query)
            status = "FAIL" if failing_rows else "OK"
            print(
                f"[{index:>{count_width}}/{total:>{count_width}}] "
                f"{test.name:<{name_width}} {status}",
                flush=True,
            )
            if not failing_rows:
                continue

            failed_tests.append(test.name)
            print(f"  source: {test.source}", flush=True)
            print(f"  failing rows: {failing_rows}", flush=True)
            columns, rows = sample_failing_rows(conn, test.query, sample_rows)
            for line in format_sample(columns, rows):
                print(f"  {line}", flush=True)

    if failed_tests:
        raise ValueError(f"{len(failed_tests)} data tests failed")


def count_failing_rows(conn: duckdb.DuckDBPyConnection, query: str) -> int:
    row = conn.execute(f"select count(*) from ({query}) as failing_rows").fetchone()
    if row is None:
        raise ValueError("Missing data test count")
    return int(row[0])


def sample_failing_rows(
    conn: duckdb.DuckDBPyConnection,
    query: str,
    limit: int,
) -> tuple[list[str], list[tuple[object, ...]]]:
    cursor = conn.execute(f"select * from ({query}) as failing_rows limit {limit}")
    rows = cursor.fetchall()
    columns = [description[0] for description in cursor.description]
    return columns, rows


def format_sample(columns: list[str], rows: list[tuple[object, ...]]) -> list[str]:
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
    return ["sample:", header, divider, *body]


def format_value(value: object) -> str:
    if value is None:
        return "null"
    return str(value)
