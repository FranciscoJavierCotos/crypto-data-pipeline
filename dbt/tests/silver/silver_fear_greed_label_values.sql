-- Silver test: fear_greed_label should be one of allowed values when present
-- A passing test returns 0 rows.

select
    id,
    metric_date,
    fear_greed_label
from {{ ref('dim_crypto_daily') }}
where fear_greed_label is not null
  and fear_greed_label not in (
    'Extreme Fear',
    'Fear',
    'Neutral',
    'Greed',
    'Extreme Greed'
  )
