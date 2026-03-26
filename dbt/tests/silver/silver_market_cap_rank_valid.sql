-- Silver test: market_cap_rank should be positive when present
-- A passing test returns 0 rows.

select
    id,
    metric_date,
    market_cap_rank
from {{ ref('dim_crypto_daily') }}
where market_cap_rank is not null
  and market_cap_rank < 1
