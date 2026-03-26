-- Silver test: Bitcoin on-chain context should exist for Bitcoin on each date with silver data
-- A passing test returns 0 rows.

{% set quality_grace_days = var('quality_grace_days', 1) | int %}

with all_dates as (
    select distinct metric_date
    from {{ ref('dim_crypto_daily') }}
    where metric_date <= date_sub(current_date, {{ quality_grace_days }})
),

btc_onchain as (
    select distinct metric_date
    from {{ ref('dim_crypto_daily') }}
    where id = 'bitcoin'
      and btc_tx_count is not null
      and btc_unique_addresses is not null
      and btc_hash_rate is not null
      and btc_mempool_size is not null
)

select d.metric_date
from all_dates d
left join btc_onchain b
    on d.metric_date = b.metric_date
where b.metric_date is null
