-- Silver test: Ensure no duplicate (id, metric_date) combinations
-- A passing test returns 0 rows.

select
    id,
    metric_date,
    count(*) as occurrences
from {{ ref('dim_crypto_daily') }}
group by id, metric_date
having count(*) > 1
