# asset.name = transformed_numbers
# asset.schema = raw
# asset.depends = raw.base_numbers
import polars as pl

import bdp


def transformed_numbers() -> pl.DataFrame:
    source_numbers = bdp.table("raw.base_numbers")
    return source_numbers.select(
        "value",
        "square",
        "is_even",
        "label",
        (pl.col("value") * 2).alias("double"),
        (pl.col("value") ** 3).alias("cube"),
        (pl.col("value") + pl.col("square")).alias("value_plus_square"),
        (
            pl
            .when(pl.col("is_even"))
            .then(pl.lit("even"))
            .otherwise(pl.lit("odd"))
            .alias("parity")
        ),
    )
