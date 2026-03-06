-- asset.schema = raw
-- asset.description = Enriched numbers from SQL materialization
-- asset.depends = raw.base_numbers
select
    value,
    square,
    label,
    case when is_even then 'even' else 'odd' end as parity,
    value * 10 as value_times_ten,
    upper(label) as label_upper
from raw.base_numbers
