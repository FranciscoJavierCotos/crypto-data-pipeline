-- Gold test: Liquidity rank should not duplicate within the same date
-- A passing test returns 0 rows.

select
    metric_date,
    liquidity_rank,
    count(*) as occurrences
from {{ ref('gold_liquidity_ranking') }}
group by metric_date, liquidity_rank
having count(*) > 1
