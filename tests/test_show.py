from pathlib import Path

import pytest

from bdp.materialize import materialize
from bdp.show import show_asset


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    project_root = tmp_path
    assets_root = project_root / "assets"
    raw_root = assets_root / "raw"
    raw_root.mkdir(parents=True)
    (project_root / "tests" / "data").mkdir(parents=True)

    monkeypatch.chdir(project_root)
    monkeypatch.setenv("BDP_DB_PATH", str(project_root / "bdp.duckdb"))

    (raw_root / "base_numbers.py").write_text(
        "\n".join([
            "# asset.description = Base numbers",
            "# asset.not_null = value",
            "# asset.unique = value",
            "# asset.assert = value > 0",
            "import polars as pl",
            "",
            "def base_numbers() -> pl.DataFrame:",
            '    return pl.DataFrame({"value": [1, 2]})',
            "",
        ]),
        encoding="utf-8",
    )
    (
        project_root / "tests" / "data" / "raw.base_numbers__has_rows.test.sql"
    ).write_text(
        "select * from raw.base_numbers where value is null",
        encoding="utf-8",
    )
    return project_root


def test_show_asset_prints_metadata_and_tests(
    project: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    show_asset("raw.base_numbers")

    captured = capsys.readouterr().out
    assert "asset: raw.base_numbers" in captured
    assert "path: assets/raw/base_numbers.py" in captured
    assert "kind: python" in captured
    assert "resolved key: raw.base_numbers" in captured
    assert "  - not_null: value" in captured
    assert "  - unique: value" in captured
    assert "  - assert: value > 0" in captured
    assert "  - custom: tests/data/raw.base_numbers__has_rows.test.sql" in captured
    assert "  not materialized" in captured


def test_show_asset_prints_sample_rows_when_materialized(
    project: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    materialize(["raw.base_numbers"])

    show_asset("raw.base_numbers")

    captured = capsys.readouterr().out
    assert "sample:" in captured
    assert "value" in captured
    assert "1" in captured
    assert "2" in captured
    assert "not materialized" not in captured


def test_show_asset_rejects_unknown_assets(project: Path) -> None:
    with pytest.raises(ValueError, match=r"Unknown asset: raw.missing"):
        show_asset("raw.missing")


def test_show_asset_validates_sample_rows(project: Path) -> None:
    with pytest.raises(ValueError, match=r"sample_rows must be at least 1"):
        show_asset("raw.base_numbers", sample_rows=0)
