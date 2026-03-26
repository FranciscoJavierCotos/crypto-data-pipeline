-- Gold test: Dominance percentages should sum to ~100% per day
-- Allows 0.01% tolerance for rounding.
-- A passing test returns 0 rows.

select
    metric_date,
    btc_dominance_pct,
    altcoin_dominance_pct,
    btc_dominance_pct + altcoin_dominance_pct as total_pct
from {{ ref('gold_btc_dominance') }}
where abs(btc_dominance_pct + altcoin_dominance_pct - 100) > 0.01
