import ast
import importlib.util
import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from graphlib import CycleError, TopologicalSorter
from pathlib import Path
from types import ModuleType
from typing import Literal

import polars as pl

from bdp.api import db_connection, find_assets_root

AssetKind = Literal["python", "sql"]
ASSET_KIND_BY_SUFFIX: dict[str, AssetKind] = {
    ".py": "python",
    ".sql": "sql",
}
COMMENT_PREFIXES: dict[AssetKind, str] = {"python": "#", "sql": "--"}
SUPPORTED_METADATA_KEYS = {"description", "depends", "not_null", "unique", "assert"}
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
METADATA_LINE_RE = re.compile(
    r"asset\.(?P<key>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?P<value>.*)"
)
ValidationReporter = Callable[[str, str], None]
PARSE_ASSETS_LABEL = "parse asset files and metadata"
UNIQUE_ASSET_KEYS_LABEL = "validate unique asset keys"
PYTHON_ENTRYPOINTS_LABEL = "validate python asset entrypoints"
DEPENDENCIES_LABEL = "validate dependencies"
DEPENDENCY_ORDERING_LABEL = "validate dependency ordering"
CHECK_STATUS_WIDTH = (
    max(
        len(PARSE_ASSETS_LABEL),
        len(UNIQUE_ASSET_KEYS_LABEL),
        len(PYTHON_ENTRYPOINTS_LABEL),
        len(DEPENDENCIES_LABEL),
        len(DEPENDENCY_ORDERING_LABEL),
    )
    + 8
)


@dataclass(frozen=True)
class AssetTests:
    not_null: tuple[str, ...]
    unique: tuple[str, ...]
    assertions: tuple[str, ...]


@dataclass(frozen=True)
class Asset:
    name: str
    schema: str
    key: str
    path: Path
    kind: AssetKind
    depends: tuple[str, ...]
    description: str | None
    tests: AssetTests


def materialize(names: Iterable[str] | None = None) -> None:
    assets = ordered_assets(names)
    materialize_assets(assets)


def materialize_assets(assets: list[Asset]) -> None:
    total = len(assets)
    count_width = len(str(total))
    asset_width = max((len(asset.key) for asset in assets), default=0)
    for index, asset in enumerate(assets, start=1):
        try:
            materialize_asset(asset)
        except Exception:
            print(
                format_materialize_status(
                    index,
                    total,
                    count_width,
                    asset_width,
                    asset,
                    "FAIL",
                ),
                flush=True,
            )
            raise
        print(
            format_materialize_status(
                index,
                total,
                count_width,
                asset_width,
                asset,
                "OK",
            ),
            flush=True,
        )


def check_assets() -> None:
    ordered_assets(reporter=print_check_status)


def ordered_assets(
    names: Iterable[str] | None = None,
    reporter: ValidationReporter | None = None,
) -> list[Asset]:
    assets, graph = validate_assets(find_assets_root(), reporter)
    selected = resolve_selection(names, assets, graph)
    selected_graph = {key: graph[key] for key in sorted(selected)}
    ordered_keys = run_validation_step(
        DEPENDENCY_ORDERING_LABEL,
        reporter,
        lambda: topological_order(selected_graph),
    )
    return [assets[key] for key in ordered_keys]


def discover_assets(assets_root: Path) -> dict[str, Asset]:
    assets, _ = validate_assets(assets_root)
    return assets


def validate_assets(
    assets_root: Path,
    reporter: ValidationReporter | None = None,
) -> tuple[dict[str, Asset], dict[str, tuple[str, ...]]]:
    assets = run_validation_step(
        PARSE_ASSETS_LABEL,
        reporter,
        lambda: collect_assets(assets_root),
    )
    indexed_assets = run_validation_step(
        UNIQUE_ASSET_KEYS_LABEL,
        reporter,
        lambda: index_assets(assets),
    )
    run_validation_step(
        PYTHON_ENTRYPOINTS_LABEL,
        reporter,
        lambda: validate_python_asset_entrypoints(indexed_assets.values()),
    )
    graph = run_validation_step(
        DEPENDENCIES_LABEL,
        reporter,
        lambda: dependency_graph(indexed_assets),
    )
    return indexed_assets, graph


def collect_assets(assets_root: Path) -> list[Asset]:
    assets: list[Asset] = []
    for path in asset_files(assets_root):
        assets.append(asset_from_path(path, assets_root))
    return assets


def dependency_graph(assets: dict[str, Asset]) -> dict[str, tuple[str, ...]]:
    graph: dict[str, tuple[str, ...]] = {}
    asset_keys = set(assets)
    for asset in assets.values():
        for dependency in asset.depends:
            if dependency == asset.key:
                raise ValueError(f"Asset {asset.key} depends on itself")
            if dependency not in asset_keys:
                raise ValueError(
                    f"Unknown dependency '{dependency}' referenced in {asset.path}"
                )
        graph[asset.key] = tuple(sorted(asset.depends))
    return graph


def index_assets(assets: Iterable[Asset]) -> dict[str, Asset]:
    indexed_assets: dict[str, Asset] = {}
    for asset in assets:
        if asset.key in indexed_assets:
            raise ValueError(f"Duplicate asset key: {asset.key}")
        indexed_assets[asset.key] = asset
    return indexed_assets


def topological_order(graph: dict[str, tuple[str, ...]]) -> list[str]:
    try:
        return list(TopologicalSorter(graph).static_order())
    except CycleError as exc:
        raise ValueError(f"Dependency cycle detected: {exc}") from exc


def resolve_selection(
    names: Iterable[str] | None,
    assets: dict[str, Asset],
    graph: dict[str, tuple[str, ...]],
) -> set[str]:
    if not names:
        selected = set(assets)
    else:
        requested = list(names)
        unknown = sorted(set(requested) - set(assets))
        if unknown:
            raise ValueError(f"Unknown assets: {', '.join(unknown)}")
        selected = set(requested)
    stack = list(selected)
    while stack:
        key = stack.pop()
        for dependency in graph[key]:
            if dependency in selected:
                continue
            selected.add(dependency)
            stack.append(dependency)
    return selected


def format_materialize_status(
    index: int,
    total: int,
    count_width: int,
    asset_width: int,
    asset: Asset,
    status: str,
) -> str:
    return (
        f"[{index:>{count_width}}/{total:>{count_width}}] "
        f"{asset.key:<{asset_width}} {status}"
    )


def run_validation_step[T](
    label: str,
    reporter: ValidationReporter | None,
    func: Callable[[], T],
) -> T:
    try:
        result = func()
    except Exception:
        report_validation_status(reporter, label, "FAIL")
        raise
    report_validation_status(reporter, label, "OK")
    return result


def report_validation_status(
    reporter: ValidationReporter | None,
    label: str,
    status: str,
) -> None:
    if reporter is None:
        return
    reporter(label, status)


def print_check_status(label: str, status: str) -> None:
    print(format_check_status(label, status), flush=True)


def format_check_status(label: str, status: str) -> str:
    return f"{label:.<{CHECK_STATUS_WIDTH}} {status}"


def asset_files(assets_root: Path) -> list[Path]:
    asset_paths: list[Path] = []
    for path in assets_root.rglob("*"):
        if not path.is_file():
            continue
        if path.name.startswith("_"):
            continue
        if "__pycache__" in path.parts:
            continue
        if path.suffix not in ASSET_KIND_BY_SUFFIX:
            continue
        asset_paths.append(path)
    return sorted(asset_paths)


def asset_from_path(path: Path, assets_root: Path) -> Asset:
    kind = asset_kind_from_path(path)
    source = path.read_text(encoding="utf-8")
    metadata, body_lines = metadata_from_source(path, kind, source)
    ensure_asset_body(body_lines, path)
    schema, name = asset_identity_from_path(path, assets_root)

    return Asset(
        name=name,
        schema=schema,
        key=f"{schema}.{name}",
        path=path,
        kind=kind,
        depends=tuple(parse_dependencies(metadata.get("depends", []), path)),
        description=optional_metadata_value(metadata, "description", path),
        tests=AssetTests(
            not_null=tuple(parse_not_null(metadata.get("not_null", []), path)),
            unique=tuple(parse_unique(metadata.get("unique", []), path)),
            assertions=tuple(parse_assertions(metadata.get("assert", []), path)),
        ),
    )


def asset_kind_from_path(path: Path) -> AssetKind:
    try:
        return ASSET_KIND_BY_SUFFIX[path.suffix]
    except KeyError as exc:
        raise ValueError(f"Unsupported asset type: {path}") from exc


def metadata_from_source(
    path: Path,
    kind: AssetKind,
    source: str,
) -> tuple[dict[str, list[str]], list[str]]:
    prefix = COMMENT_PREFIXES[kind]
    metadata_lines, body_lines = extract_metadata_lines(source, prefix)
    return parse_metadata_lines(metadata_lines, path), body_lines


def extract_metadata_lines(source: str, prefix: str) -> tuple[list[str], list[str]]:
    metadata_lines: list[str] = []
    source_lines = source.splitlines()
    body_start = len(source_lines)
    for index, line in enumerate(source_lines):
        stripped = line.lstrip()
        if not stripped:
            if not metadata_lines:
                continue
            continue
        if stripped.startswith("#!") and not metadata_lines:
            continue
        if stripped.startswith(prefix):
            content = stripped[len(prefix) :].lstrip()
            if content:
                metadata_lines.append(content)
            continue
        body_start = index
        break
    if body_start >= len(source_lines):
        return metadata_lines, []
    return metadata_lines, source_lines[body_start:]


def parse_metadata_lines(lines: list[str], path: Path) -> dict[str, list[str]]:
    metadata: dict[str, list[str]] = {}
    for line in lines:
        if not line.startswith("asset."):
            continue
        match = METADATA_LINE_RE.fullmatch(line)
        if match is None:
            raise ValueError(f"Invalid asset metadata line in {path}: {line}")
        key = match.group("key")
        if key not in SUPPORTED_METADATA_KEYS:
            raise ValueError(unsupported_metadata_message(key, path))
        value = match.group("value").strip()
        metadata.setdefault(key, []).append(value)
    return metadata


def unsupported_metadata_message(key: str, path: Path) -> str:
    if key == "schema":
        return (
            f"Unsupported asset.schema in {path}. "
            "Schema comes from the first folder under assets."
        )
    if key == "name":
        return (
            f"Unsupported asset.name in {path}. Table names come from the asset path."
        )
    return f"Unsupported asset.{key} in {path}"


def optional_metadata_value(
    metadata: dict[str, list[str]],
    key: str,
    path: Path,
) -> str | None:
    values = metadata.get(key, [])
    if not values:
        return None
    if len(values) != 1:
        raise ValueError(f"asset.{key} must appear once in {path}")
    value = values[0]
    if not value:
        raise ValueError(f"asset.{key} must have a value in {path}")
    return value


def parse_dependencies(values: list[str], path: Path) -> list[str]:
    dependencies: list[str] = []
    seen: set[str] = set()
    for raw_value in values:
        dependency = parse_single_metadata_value(
            raw_value,
            path,
            "depends",
            label="dependency",
        )
        if dependency in seen:
            raise ValueError(f"Duplicate dependency '{dependency}' in {path}")
        validate_asset_reference(dependency, path)
        seen.add(dependency)
        dependencies.append(dependency)
    return dependencies


def parse_not_null(values: list[str], path: Path) -> list[str]:
    columns: list[str] = []
    seen: set[str] = set()
    for raw_value in values:
        column = parse_single_column_metadata(raw_value, path, "not_null")
        if column in seen:
            raise ValueError(f"Duplicate asset.not_null column '{column}' in {path}")
        seen.add(column)
        columns.append(column)
    return columns


def parse_unique(values: list[str], path: Path) -> list[str]:
    columns: list[str] = []
    seen: set[str] = set()
    for raw_value in values:
        column = parse_single_column_metadata(raw_value, path, "unique")
        if column in seen:
            raise ValueError(f"Duplicate asset.unique column '{column}' in {path}")
        seen.add(column)
        columns.append(column)
    return columns


def parse_assertions(values: list[str], path: Path) -> list[str]:
    assertions: list[str] = []
    seen: set[str] = set()
    for raw_value in values:
        assertion = raw_value.strip()
        if not assertion:
            raise ValueError(f"asset.assert must have a value in {path}")
        if assertion in seen:
            raise ValueError(f"Duplicate asset.assert '{assertion}' in {path}")
        seen.add(assertion)
        assertions.append(assertion)
    return assertions


def parse_single_metadata_value(
    raw_value: str,
    path: Path,
    key: str,
    *,
    label: str,
) -> str:
    value = raw_value.strip()
    if not value:
        raise ValueError(f"asset.{key} must have a value in {path}")
    if "," in value:
        raise ValueError(
            f"asset.{key} must declare one {label} per line in {path}: {value}"
        )
    return value


def parse_single_column_metadata(raw_value: str, path: Path, key: str) -> str:
    column = parse_single_metadata_value(raw_value, path, key, label="column")
    validate_identifier(column, "column", path)
    return column


def ensure_asset_body(body_lines: list[str], path: Path) -> None:
    for line in body_lines:
        if line.strip():
            return
    raise ValueError(f"Asset file has no content beyond metadata: {path}")


def asset_identity_from_path(path: Path, assets_root: Path) -> tuple[str, str]:
    path_parts = path.relative_to(assets_root).with_suffix("").parts
    if len(path_parts) < 2:
        raise ValueError(
            f"Asset path must include a schema folder under assets: {path}"
        )

    schema, *table_parts = path_parts
    validate_identifier(schema, "schema", path)
    for part in table_parts:
        validate_identifier(part, "table", path)

    name = "_".join(table_parts)
    return schema, name


def validate_asset_reference(value: str, path: Path) -> None:
    parts = value.split(".")
    if len(parts) != 2:
        raise ValueError(
            f"Invalid dependency '{value}' in {path}. Expected schema.table."
        )
    schema, table = parts
    validate_identifier(schema, "schema", path)
    validate_identifier(table, "table", path)


def validate_identifier(value: str, label: str, path: Path) -> None:
    if IDENTIFIER_RE.fullmatch(value) is None:
        raise ValueError(f"Invalid {label} name '{value}' from {path}")


def validate_python_asset_entrypoints(assets: Iterable[Asset]) -> None:
    for asset in assets:
        if asset.kind != "python":
            continue
        validate_python_asset_source(asset.path)


def validate_python_asset_source(path: Path) -> None:
    function_name = python_asset_function_name(path)
    source = path.read_text(encoding="utf-8")
    try:
        module = ast.parse(source, filename=str(path))
    except SyntaxError as exc:
        raise ValueError(invalid_python_asset_message(path, exc)) from exc

    for node in module.body:
        if (
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == function_name
        ):
            return

    raise ValueError(
        f"Python asset {path} must define top-level function {function_name}"
    )


def invalid_python_asset_message(path: Path, error: SyntaxError) -> str:
    location = ""
    if error.lineno is not None:
        location = f" at line {error.lineno}"
        if error.offset is not None:
            location = f"{location}, column {error.offset}"
    return f"Invalid Python asset {path}: {error.msg}{location}"


def python_asset_function_name(path: Path) -> str:
    return path.stem


def materialize_asset(asset: Asset) -> None:
    if asset.kind == "python":
        materialize_python(asset)
        return
    materialize_sql(asset)


def materialize_sql(asset: Asset) -> None:
    query = asset.path.read_text(encoding="utf-8").strip()
    if not query:
        raise ValueError(f"SQL asset is empty: {asset.path}")
    with db_connection() as conn:
        conn.execute(f"create schema if not exists {asset.schema}")
        conn.execute(f"create or replace table {asset.key} as {query}")
        comment_on_table(conn, asset)


def materialize_python(asset: Asset) -> None:
    module = load_module(asset.path)
    function_name = python_asset_function_name(asset.path)
    func = getattr(module, function_name, None)
    if func is None or not callable(func):
        raise ValueError(
            f"Python asset {asset.path} must define callable {function_name}"
        )
    result = func()
    if not isinstance(result, pl.DataFrame):
        raise TypeError("Python assets must return polars.DataFrame")
    with db_connection() as conn:
        conn.execute(f"create schema if not exists {asset.schema}")
        conn.register("frame", result)
        conn.execute(f"create or replace table {asset.key} as select * from frame")
        comment_on_table(conn, asset)


def comment_on_table(
    conn,
    asset: Asset,
) -> None:
    if not asset.description:
        return
    escaped = asset.description.replace("'", "''")
    conn.execute(f"comment on table {asset.key} is '{escaped}'")


def load_module(module_path: Path) -> ModuleType:
    module_name = module_name_from_path(module_path)
    module_spec = importlib.util.spec_from_file_location(module_name, module_path)
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError(f"Unable to load asset module: {module_path}")
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    return module


def module_name_from_path(module_path: Path) -> str:
    sanitized = module_path.as_posix().replace("/", "_").replace(".", "_")
    return f"bdp_asset_{sanitized}"
