-- Silver test: volume_to_market_cap_ratio should be within sane bounds when present
-- A passing test returns 0 rows.

select
    id,
    metric_date,
    volume_to_market_cap_ratio
from {{ ref('dim_crypto_daily') }}
where volume_to_market_cap_ratio is not null
  and (
    volume_to_market_cap_ratio < 0
    or volume_to_market_cap_ratio > 10
  )
