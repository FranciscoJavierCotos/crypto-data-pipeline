-- Gold test: Liquidity rank should always be >= 1
-- A passing test returns 0 rows.

select
    id,
    metric_date,
    liquidity_rank
from {{ ref('gold_liquidity_ranking') }}
where liquidity_rank < 1
