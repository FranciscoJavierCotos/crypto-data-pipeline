{% set run_key = var('run_key', '') %}

with invalid_metrics as (
    select
    run_key,
        id,
        current_price,
        market_cap,
        total_volume,
        high_24h,
        low_24h
    from {{ source('bronze', 'coingecko_market_data') }}
    where run_key = '{{ run_key }}'
      and (
        current_price < 0
        or market_cap < 0
        or total_volume < 0
        or high_24h < 0
        or low_24h < 0
      )
)

select *
from invalid_metrics
