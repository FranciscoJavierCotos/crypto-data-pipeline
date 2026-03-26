-- Gold test: Volatility bucket values must be one of the defined categories
-- A passing test returns 0 rows.

select
    id,
    metric_date,
    volatility_bucket
from {{ ref('gold_volatility_signal') }}
where volatility_bucket not in ('low', 'medium', 'high', 'extreme', 'insufficient_data')
