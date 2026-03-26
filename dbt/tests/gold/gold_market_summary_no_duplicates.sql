-- Gold test: Market summary should have no duplicate (id, metric_date) pairs
-- A passing test returns 0 rows.

select
    id,
    metric_date,
    count(*) as occurrences
from {{ ref('gold_market_summary') }}
group by id, metric_date
having count(*) > 1
