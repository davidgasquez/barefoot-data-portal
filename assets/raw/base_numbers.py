# asset.description = Base numbers for demos
# asset.not_null = value
# asset.not_null = square
# asset.not_null = is_even
# asset.not_null = label
# asset.unique = value
# asset.assert = square = value * value
import polars as pl


def base_numbers() -> pl.DataFrame:
    return pl.DataFrame({
        "value": [1, 2, 3, 4],
        "square": [1, 4, 9, 16],
        "is_even": [False, True, False, True],
        "label": ["one", "two", "three", "four"],
    })
