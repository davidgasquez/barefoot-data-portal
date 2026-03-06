import html
from pathlib import Path

import duckdb

from bdp.api import db_connection, find_assets_root
from bdp.materialize import Asset, discover_assets

FAVICON_HREF = (
    "data:image/svg+xml,%3Csvg xmlns=%27http://www.w3.org/2000/svg%27 "
    "viewBox=%270 0 100 100%27%3E%3Crect width=%27100%27 height=%27100%27 "
    "fill=%27white%27/%3E%3Ctext x=%2750%27 y=%2762%27 font-size=%2748%27 "
    "text-anchor=%27middle%27 fill=%27black%27 font-family=%27monospace%27%3E"
    "bdp%3C/text%3E%3C/svg%3E"
)
BODY_FONT = (
    "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "
    "'Liberation Mono', 'Courier New', monospace"
)


def generate_docs(out_path: Path | str, sample_rows: int = 10) -> None:
    output_path = Path(out_path)
    assets_root = find_assets_root()
    assets = discover_assets(assets_root)
    if not assets:
        raise ValueError("No assets found.")

    sorted_assets = [assets[key] for key in sorted(assets)]
    rendered_assets: list[str] = []
    with db_connection() as conn:
        for asset in sorted_assets:
            if not table_exists(conn, asset):
                raise ValueError(f"Missing table {asset.key}. Run `bdp materialize`.")
            columns = fetch_columns(conn, asset)
            row_count = fetch_row_count(conn, asset)
            sample_columns, sample_values = fetch_sample_rows(conn, asset, sample_rows)
            rendered_assets.append(
                render_asset_section(
                    asset,
                    assets_root,
                    columns,
                    row_count,
                    sample_columns,
                    sample_values,
                )
            )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_document(rendered_assets, sorted_assets),
        encoding="utf-8",
    )


def table_exists(conn: duckdb.DuckDBPyConnection, asset: Asset) -> bool:
    row = conn.execute(
        "select 1 from information_schema.tables "
        "where table_schema = ? and table_name = ? limit 1",
        [asset.schema, asset.name],
    ).fetchone()
    return row is not None


def fetch_columns(
    conn: duckdb.DuckDBPyConnection,
    asset: Asset,
) -> list[tuple[str, str]]:
    return conn.execute(
        "select column_name, data_type "
        "from information_schema.columns "
        "where table_schema = ? and table_name = ? "
        "order by ordinal_position",
        [asset.schema, asset.name],
    ).fetchall()


def fetch_row_count(conn: duckdb.DuckDBPyConnection, asset: Asset) -> int:
    row = conn.execute(f"select count(*) from {asset.key}").fetchone()
    if row is None:
        raise ValueError(f"Missing count for {asset.key}")
    return int(row[0])


def fetch_sample_rows(
    conn: duckdb.DuckDBPyConnection,
    asset: Asset,
    limit: int,
) -> tuple[list[str], list[tuple[object, ...]]]:
    cursor = conn.execute(f"select * from {asset.key} limit {limit}")
    rows = cursor.fetchall()
    columns = [description[0] for description in cursor.description]
    return columns, rows


def render_document(rendered_assets: list[str], assets: list[Asset]) -> str:
    index_html = "\n".join([
        "<ul>",
        *[
            "  <li>"
            f'<a href="#{html.escape(asset.key)}">'
            f"{html.escape(asset.key)}</a>"
            "</li>"
            for asset in assets
        ],
        "</ul>",
    ])
    sections_html = "\n".join(rendered_assets)
    return "\n".join([
        "<!doctype html>",
        '<html lang="en">',
        "<head>",
        '  <meta charset="utf-8">',
        '  <meta name="viewport" content="width=device-width, initial-scale=1">',
        "  <title>BDP Docs</title>",
        f'  <link rel="icon" href="{FAVICON_HREF}">',
        "  <style>",
        "    :root { color-scheme: light }",
        f"    body {{ font-family: {BODY_FONT} }}",
        "    body { background: #fff }",
        "    body { color: #111 }",
        "    body { margin: 0 }",
        "    body { padding: 24px }",
        "    body { line-height: 1.5 }",
        "    .layout { display: grid }",
        "    .layout { grid-template-columns: 240px 1fr }",
        "    .layout { gap: 24px }",
        "    .layout { align-items: start }",
        "    main { max-width: 980px }",
        "    main { min-width: 0 }",
        "    h1 { font-size: 24px }",
        "    h1 { margin: 0 0 8px }",
        "    h2 { font-size: 16px }",
        "    h2 { margin: 20px 0 8px }",
        "    h3 { font-size: 14px }",
        "    h3 { margin: 16px 0 6px }",
        "    p { margin: 0 0 12px }",
        "    .hero { margin: 0 0 24px }",
        "    .intro { color: #444 }",
        "    .intro { max-width: 720px }",
        "    aside { position: sticky }",
        "    aside { top: 24px }",
        "    ul { list-style: none }",
        "    ul { padding: 0 }",
        "    ul { margin: 8px 0 0 }",
        "    li { margin: 4px 0 }",
        "    a { color: inherit }",
        "    a { text-decoration: none }",
        "    a { border-bottom: 1px solid #ddd }",
        "    a:hover { border-bottom-color: #111 }",
        "    section { background: #fafafa }",
        "    section { border: 1px solid #eee }",
        "    section { border-radius: 6px }",
        "    section { padding: 16px }",
        "    section { margin: 0 0 16px }",
        "    table { border-collapse: collapse }",
        "    table { width: 100% }",
        "    table { margin: 8px 0 16px }",
        "    th, td { text-align: left }",
        "    th, td { padding: 4px 6px }",
        "    th, td { border-bottom: 1px solid #eee }",
        "    th, td { vertical-align: top }",
        "    th { font-weight: 600 }",
        "    .small { color: #666 }",
        "    .small { font-size: 12px }",
        "    code { background: #f6f6f6 }",
        "    code { padding: 2px 4px }",
        "    code { border-radius: 4px }",
        "    @media (max-width: 900px) {",
        "      .layout { grid-template-columns: 1fr }",
        "      aside { position: static }",
        "    }",
        "  </style>",
        "</head>",
        "<body>",
        '<div class="layout">',
        "  <aside>",
        '    <div class="small">Assets</div>',
        f"    {index_html}",
        "  </aside>",
        "  <main>",
        '    <div class="hero">',
        '      <div class="small">Barefoot Data Platform</div>',
        "      <h1>Asset docs</h1>",
        '      <p class="intro">Generated documentation for materialized '
        "assets. Run bdp docs after materialize.</p>",
        "    </div>",
        f"    {sections_html}",
        "  </main>",
        "</div>",
        "</body>",
        "</html>",
    ])


def render_asset_section(
    asset: Asset,
    assets_root: Path,
    columns: list[tuple[str, str]],
    row_count: int,
    sample_columns: list[str],
    sample_values: list[tuple[object, ...]],
) -> str:
    description_html = (
        f"<p>{html.escape(asset.description)}</p>"
        if asset.description
        else '<div class="small">No description.</div>'
    )
    rel_path = html.escape(asset.path.relative_to(assets_root).as_posix())
    kind = html.escape(asset.kind)
    return "\n".join([
        f'<section id="{html.escape(asset.key)}">',
        f"  <h2>{html.escape(asset.key)}</h2>",
        f'  <div class="small">{kind} · {rel_path}</div>',
        f"  {description_html}",
        "  <h3>Definition</h3>",
        f"  {render_asset_table(asset)}",
        "  <h3>Columns</h3>",
        f"  {render_columns_table(columns)}",
        f'  <div class="small">Rows: {row_count}</div>',
        "  <h3>Sample</h3>",
        f"  {render_sample_table(sample_columns, sample_values)}",
        "</section>",
    ])


def render_asset_table(asset: Asset) -> str:
    depends_html = render_depends_value(asset.depends)
    rows = [
        ["<code>schema</code>", html.escape(asset.schema)],
        ["<code>table</code>", f"<code>{html.escape(asset.name)}</code>"],
        ["<code>depends</code>", depends_html],
    ]
    return render_table(["Field", "Value"], rows)


def render_columns_table(columns: list[tuple[str, str]]) -> str:
    if not columns:
        return '<div class="small">No columns.</div>'
    rows = [[html.escape(name), html.escape(dtype)] for name, dtype in columns]
    return render_table(["Column", "Type"], rows)


def render_sample_table(
    columns: list[str],
    rows: list[tuple[object, ...]],
) -> str:
    if not columns:
        return '<div class="small">No sample available.</div>'
    if not rows:
        return '<div class="small">No rows.</div>'
    body_rows = [[html.escape(format_value(value)) for value in row] for row in rows]
    return render_table(columns, body_rows)


def render_table(headers: list[str], rows: list[list[str]]) -> str:
    head_html = "".join(f"<th>{html.escape(header)}</th>" for header in headers)
    body_html = "\n".join([
        "    <tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>"
        for row in rows
    ])
    return "\n".join([
        "  <table>",
        "    <thead>",
        f"      <tr>{head_html}</tr>",
        "    </thead>",
        "    <tbody>",
        body_html,
        "    </tbody>",
        "  </table>",
    ])


def render_depends_value(dependencies: tuple[str, ...]) -> str:
    if not dependencies:
        return '<span class="small">None</span>'
    links = [
        f'<a href="#{html.escape(dependency)}">{html.escape(dependency)}</a>'
        for dependency in dependencies
    ]
    return ", ".join(links)


def format_value(value: object) -> str:
    if value is None:
        return "null"
    return str(value)
