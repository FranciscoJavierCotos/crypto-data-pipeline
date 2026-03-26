-- Silver test: core asset identifiers should always be present
-- A passing test returns 0 rows.

select
    id,
    symbol,
    name,
    metric_date
from {{ ref('dim_crypto_daily') }}
where trim(coalesce(id, '')) = ''
   or trim(coalesce(symbol, '')) = ''
   or trim(coalesce(name, '')) = ''
