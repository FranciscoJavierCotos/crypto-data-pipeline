-- Silver test: Fear & Greed value should be 0-100 when present
-- A passing test returns 0 rows.

select
    id,
    metric_date,
    fear_greed_value
from {{ ref('dim_crypto_daily') }}
where fear_greed_value is not null
  and (fear_greed_value < 0 or fear_greed_value > 100)
