-- asset.name = filtered_numbers
-- asset.schema = raw
-- asset.depends = raw.transformed_numbers
select
    value,
    square,
    label,
    double,
    parity
from raw.transformed_numbers
where value >= 3
