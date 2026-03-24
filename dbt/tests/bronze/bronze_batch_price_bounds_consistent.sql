{% set run_key = var('run_key', '') %}

with invalid_bounds as (
    select
        run_key,
        id,
        high_24h,
        low_24h
    from {{ source('bronze', 'coingecko_market_data') }}
    where run_key = '{{ run_key }}'
      and high_24h is not null
      and low_24h is not null
      and high_24h < low_24h
)

select *
from invalid_bounds
