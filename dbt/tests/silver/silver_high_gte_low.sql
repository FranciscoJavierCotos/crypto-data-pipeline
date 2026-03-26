-- Silver test: high_24h should be >= low_24h when both are present
-- A passing test returns 0 rows.

select
    id,
    metric_date,
    high_24h,
    low_24h
from {{ ref('dim_crypto_daily') }}
where high_24h is not null
  and low_24h is not null
  and high_24h < low_24h
