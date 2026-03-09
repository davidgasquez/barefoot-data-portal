from collections.abc import Callable
from pathlib import Path

import pytest

from bdp.materialize import (
    asset_from_path,
    parse_dependencies,
    parse_not_null,
    parse_unique,
)


def test_asset_from_path_parses_repeated_single_value_metadata(tmp_path: Path) -> None:
    assets_root = tmp_path / "assets"
    asset_path = assets_root / "raw" / "example.py"
    asset_path.parent.mkdir(parents=True)
    asset_path.write_text(
        "\n".join([
            "# asset.depends = raw.base_numbers",
            "# asset.depends = raw.other_numbers",
            "# asset.not_null = value",
            "# asset.not_null = label",
            "# asset.unique = value",
            "# asset.unique = label",
            "# asset.assert = value > 0",
            "import polars as pl",
            "",
            "def example() -> pl.DataFrame:",
            '    return pl.DataFrame({"value": [1], "label": ["one"]})',
            "",
        ]),
        encoding="utf-8",
    )

    asset = asset_from_path(asset_path, assets_root)

    assert asset.depends == ("raw.base_numbers", "raw.other_numbers")
    assert asset.tests.not_null == ("value", "label")
    assert asset.tests.unique == ("value", "label")
    assert asset.tests.assertions == ("value > 0",)


@pytest.mark.parametrize(
    ("parser", "value", "message"),
    [
        (parse_dependencies, "raw.base_numbers, raw.other_numbers", "one dependency"),
        (parse_not_null, "value, label", "one column"),
        (parse_unique, "value, label", "one column"),
    ],
)
def test_comma_separated_metadata_is_rejected(
    parser: Callable[[list[str], Path], list[str]],
    value: str,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        parser([value], Path("asset.py"))
