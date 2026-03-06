-- asset.schema = raw
-- asset.description = Filtered values from transformed numbers
-- asset.depends = raw.transformed_numbers
select
    value,
    square,
    label,
    double,
    parity
from raw.transformed_numbers
where value >= 3
