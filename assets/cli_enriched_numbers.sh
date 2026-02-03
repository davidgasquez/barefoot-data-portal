#!/usr/bin/env bash
# asset.name = cli_enriched_numbers
# asset.schema = raw
# asset.depends = raw.base_numbers

set -euo pipefail

duckdb "${BDP_DB_PATH}" <<SQL
create or replace table ${BDP_SCHEMA}.${BDP_TABLE} as
select
    value,
    square,
    label,
    case when is_even then 'even' else 'odd' end as parity,
    value * 10 as value_times_ten,
    upper(label) as label_upper
from raw.base_numbers
SQL
