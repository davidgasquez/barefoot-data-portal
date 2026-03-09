-- asset.description = Filtered values from transformed numbers
-- asset.depends = raw.transformed_numbers
-- asset.not_null = square
select
    value,
    square,
    label,
    double,
    parity
from raw.transformed_numbers
where value >= 3
