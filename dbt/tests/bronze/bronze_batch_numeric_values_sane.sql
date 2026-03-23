{% set batch_id = var('batch_id', '') %}

with invalid_metrics as (
    select
        batch_id,
        id,
        current_price,
        market_cap,
        total_volume,
        high_24h,
        low_24h
    from {{ source('bronze', 'coingecko_market_data') }}
    where batch_id = '{{ batch_id }}'
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
