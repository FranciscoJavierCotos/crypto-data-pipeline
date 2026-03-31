-- Singular test: BTC + altcoin dominance must sum to ~100% per date.
-- Tolerance: 0.01% to account for floating-point rounding.
-- Returns violating dates where the sum deviates beyond tolerance.

select
    metric_date,
    btc_dominance_pct,
    altcoin_dominance_pct,
    btc_dominance_pct + altcoin_dominance_pct as total_dominance_pct,
    abs(btc_dominance_pct + altcoin_dominance_pct - 100) as deviation
from {{ ref('gold_btc_dominance') }}
where btc_dominance_pct is not null
  and altcoin_dominance_pct is not null
  and abs(btc_dominance_pct + altcoin_dominance_pct - 100) > 0.01
