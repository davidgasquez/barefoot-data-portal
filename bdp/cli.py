import argparse
from pathlib import Path

from bdp.api import find_assets_root
from bdp.docs import generate_docs
from bdp.materialize import check_assets, discover_assets, materialize


def _materialize(args: argparse.Namespace) -> None:
    materialize(args.assets)


def _check(_: argparse.Namespace) -> None:
    check_assets()
    print("Assets: OK")


def _list_assets(_: argparse.Namespace) -> None:
    assets_root = find_assets_root()
    assets = discover_assets(assets_root)
    if not assets:
        print("No assets found.")
        return
    for key in sorted(assets):
        asset = assets[key]
        rel_path = asset.path.relative_to(assets_root)
        print(f"- {asset.key} [{asset.kind}] ({rel_path})")
        if asset.description:
            print(f"  description: {asset.description}")
        for index, dependency in enumerate(asset.depends):
            connector = "└─" if index == len(asset.depends) - 1 else "├─"
            print(f"  {connector} {dependency}")


def _docs(args: argparse.Namespace) -> None:
    generate_docs(Path(args.out), sample_rows=args.sample_rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="bdp",
        description="Barefoot Data Platform CLI.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    materialize_parser = subparsers.add_parser(
        "materialize",
        help="Materialize assets into DuckDB.",
        description="Materialize assets into DuckDB. Defaults to all assets.",
    )
    materialize_parser.add_argument(
        "assets",
        nargs="*",
        metavar="ASSET",
        help="Asset keys to materialize. Defaults to all assets.",
    )
    materialize_parser.set_defaults(func=_materialize)

    check_parser = subparsers.add_parser(
        "check",
        help="Validate assets.",
    )
    check_parser.set_defaults(func=_check)

    list_parser = subparsers.add_parser(
        "list",
        help="List available assets.",
    )
    list_parser.set_defaults(func=_list_assets)

    docs_parser = subparsers.add_parser(
        "docs",
        help="Generate asset documentation.",
    )
    docs_parser.add_argument(
        "--out",
        default="index.html",
        help="Output HTML file. Defaults to index.html.",
    )
    docs_parser.add_argument(
        "--sample-rows",
        type=int,
        default=10,
        help="Number of sample rows per asset.",
    )
    docs_parser.set_defaults(func=_docs)

    args = parser.parse_args()
    args.func(args)
