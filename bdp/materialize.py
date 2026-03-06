import importlib.util
import re
from collections.abc import Iterable
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
SUPPORTED_METADATA_KEYS = {"schema", "description", "depends"}
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
METADATA_LINE_RE = re.compile(
    r"asset\.(?P<key>[A-Za-z_][A-Za-z0-9_]*)\s*=\s*(?P<value>.*)"
)


@dataclass(frozen=True)
class Asset:
    name: str
    schema: str
    key: str
    path: Path
    kind: AssetKind
    depends: tuple[str, ...]
    description: str | None


def materialize(names: Iterable[str] | None = None) -> None:
    assets = discover_assets(find_assets_root())
    graph = dependency_graph(assets)
    for key in materialization_order(names, assets, graph):
        asset = assets[key]
        if asset.kind == "python":
            materialize_python(asset)
            continue
        materialize_sql(asset)


def check_assets() -> None:
    assets = discover_assets(find_assets_root())
    graph = dependency_graph(assets)
    topological_order(graph)


def discover_assets(assets_root: Path) -> dict[str, Asset]:
    assets: dict[str, Asset] = {}
    for path in asset_files(assets_root):
        asset = asset_from_path(path)
        if asset.key in assets:
            raise ValueError(f"Duplicate asset key: {asset.key}")
        assets[asset.key] = asset
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


def materialization_order(
    names: Iterable[str] | None,
    assets: dict[str, Asset],
    graph: dict[str, tuple[str, ...]],
) -> list[str]:
    selected = resolve_selection(names, assets, graph)
    selected_graph = {key: graph[key] for key in sorted(selected)}
    return topological_order(selected_graph)


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


def asset_from_path(path: Path) -> Asset:
    kind = asset_kind_from_path(path)
    source = path.read_text(encoding="utf-8")
    metadata, body_lines = metadata_from_source(path, kind, source)
    ensure_asset_body(body_lines, path)

    name = path.stem
    schema = required_metadata_value(metadata, "schema", path)
    validate_identifier(name, "table", path)
    validate_identifier(schema, "schema", path)

    return Asset(
        name=name,
        schema=schema,
        key=f"{schema}.{name}",
        path=path,
        kind=kind,
        depends=tuple(parse_dependencies(metadata.get("depends", []), path)),
        description=optional_metadata_value(metadata, "description", path),
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
    if not metadata_lines:
        raise ValueError(f"Missing asset metadata in {path}")
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
    if key == "name":
        return f"Unsupported asset.name in {path}. Table names come from the file name."
    return f"Unsupported asset.{key} in {path}"


def required_metadata_value(
    metadata: dict[str, list[str]],
    key: str,
    path: Path,
) -> str:
    values = metadata.get(key, [])
    if not values:
        raise ValueError(f"Missing asset.{key} in {path}")
    if len(values) != 1:
        raise ValueError(f"asset.{key} must appear once in {path}")
    value = values[0]
    if not value:
        raise ValueError(f"asset.{key} must have a value in {path}")
    return value


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
        if not raw_value:
            continue
        parts = [part.strip() for part in raw_value.split(",")]
        for dependency in parts:
            if not dependency:
                continue
            if dependency in seen:
                raise ValueError(f"Duplicate dependency '{dependency}' in {path}")
            validate_asset_reference(dependency, path)
            seen.add(dependency)
            dependencies.append(dependency)
    return dependencies


def ensure_asset_body(body_lines: list[str], path: Path) -> None:
    for line in body_lines:
        if line.strip():
            return
    raise ValueError(f"Asset file has no content beyond metadata: {path}")


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
    func = getattr(module, asset.name, None)
    if func is None or not callable(func):
        raise ValueError(f"Python asset {asset.path} must define callable {asset.name}")
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
