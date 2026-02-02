from __future__ import annotations

import argparse
import sys

from bdp.api import find_datasets_root
from bdp.materialize import discover_assets, materialize

SHORT_HELP = """Barefoot Data Platform CLI.

Usage:
  bdp list
  bdp materialize --all
  bdp materialize ASSET [ASSET...]

Run "bdp --help" for more.
"""


def _materialize(args: argparse.Namespace) -> None:
    if not args.all and not args.assets:
        raise SystemExit(
            "Usage: bdp materialize [--all] [ASSET...]\n"
            "\n"
            "Pass --all or asset names. "
            "Run 'bdp materialize --help' for more."
        )
    materialize(
        args.assets,
        all_assets=args.all,
    )


def _list_assets(_: argparse.Namespace) -> None:
    datasets_root = find_datasets_root()
    assets = discover_assets(datasets_root)
    if not assets:
        print("No assets found.")
        return
    for name in sorted(assets):
        asset = assets[name]
        rel_path = asset.path.relative_to(datasets_root)
        print(f"- {asset.name} [{asset.kind}] ({rel_path})")
        for index, dep in enumerate(asset.depends):
            connector = "└─" if index == len(asset.depends) - 1 else "├─"
            print(f"  {connector} {dep}")


def main() -> None:
    if len(sys.argv) == 1:
        print(SHORT_HELP)
        raise SystemExit(0)

    parser = argparse.ArgumentParser(
        prog="bdp",
        description="Barefoot Data Platform CLI.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    materialize_parser = subparsers.add_parser(
        "materialize",
        help="Materialize assets into DuckDB.",
    )
    materialize_parser.add_argument(
        "assets",
        nargs="*",
        metavar="ASSET",
        help="Asset names to materialize.",
    )
    materialize_parser.add_argument(
        "-a",
        "--all",
        action="store_true",
        help="Materialize all assets.",
    )
    materialize_parser.set_defaults(func=_materialize)

    list_parser = subparsers.add_parser(
        "list",
        help="List available assets.",
    )
    list_parser.set_defaults(func=_list_assets)

    args = parser.parse_args()
    args.func(args)
