# asset.schema = raw
# asset.description = Base numbers for demos
import polars as pl


def base_numbers() -> pl.DataFrame:
    return pl.DataFrame({
        "value": [1, 2, 3, 4],
        "square": [1, 4, 9, 16],
        "is_even": [False, True, False, True],
        "label": ["one", "two", "three", "four"],
    })
