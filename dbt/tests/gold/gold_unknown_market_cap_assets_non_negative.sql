-- Gold test: unknown/known market cap asset counters should be non-negative
-- A passing test returns 0 rows.

select
    metric_date,
    unknown_market_cap_asset_count,
    known_market_cap_asset_count,
    num_coins
from {{ ref('gold_btc_dominance') }}
where unknown_market_cap_asset_count < 0
   or known_market_cap_asset_count < 0
   or num_coins < 0
   or unknown_market_cap_asset_count + known_market_cap_asset_count > num_coins
