-- Silver test: Fear & Greed context should exist for Bitcoin on each date with silver data
-- A passing test returns 0 rows.

{% set quality_grace_days = var('quality_grace_days', 1) | int %}

with all_dates as (
    select distinct metric_date
    from {{ ref('dim_crypto_daily') }}
    where metric_date <= date_sub(current_date, {{ quality_grace_days }})
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
