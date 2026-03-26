-- Silver test: Price and market cap must be positive for real assets
-- Excludes stablecoins with near-zero market cap.
-- A passing test returns 0 rows.

select
    id,
    metric_date,
    current_price,
    market_cap
from {{ ref('dim_crypto_daily') }}
where current_price <= 0
   or (market_cap is not null and market_cap < 0)
