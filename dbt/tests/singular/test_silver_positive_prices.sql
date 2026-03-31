-- Singular test: all price-related columns in silver should be positive
-- when they are not null. Covers current_price, market_cap, total_volume,
-- high_24h, low_24h.

select
    id,
    metric_date,
    current_price,
    market_cap,
    total_volume,
    high_24h,
    low_24h
from {{ ref('dim_crypto_daily') }}
where (current_price is not null and current_price < 0)
   or (market_cap is not null and market_cap < 0)
   or (total_volume is not null and total_volume < 0)
   or (high_24h is not null and high_24h < 0)
   or (low_24h is not null and low_24h < 0)
