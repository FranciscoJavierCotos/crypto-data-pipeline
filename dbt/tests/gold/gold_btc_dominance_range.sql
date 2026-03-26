-- Gold test: BTC dominance should be between 0 and 100 percent
-- A passing test returns 0 rows.

select
    metric_date,
    btc_dominance_pct
from {{ ref('gold_btc_dominance') }}
where btc_dominance_pct < 0
   or btc_dominance_pct > 100
