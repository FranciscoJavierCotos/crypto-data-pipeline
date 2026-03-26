-- Silver test: Fear & Greed context should exist for Bitcoin on each date with silver data
-- A passing test returns 0 rows.

with all_dates as (
    select distinct metric_date
    from {{ ref('dim_crypto_daily') }}
),

btc_context as (
    select distinct metric_date
    from {{ ref('dim_crypto_daily') }}
    where id = 'bitcoin'
      and fear_greed_value is not null
)

select d.metric_date
from all_dates d
left join btc_context c
    on d.metric_date = c.metric_date
where c.metric_date is null
