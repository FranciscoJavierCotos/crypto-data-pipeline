-- Gold test: BTC price_in_btc should be exactly 1.0 (or very close due to rounding)
-- A passing test returns 0 rows.

select
    metric_date,
    price_in_btc
from {{ ref('gold_market_summary') }}
where id = 'bitcoin'
  and price_in_btc is not null
  and abs(price_in_btc - 1.0) > 0.0001
