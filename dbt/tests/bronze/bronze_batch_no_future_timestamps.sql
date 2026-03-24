{% set run_key = var('run_key', '') %}

with invalid_timestamps as (
    select
        run_key,
        id,
        ath_date,
        atl_date,
        last_updated,
        ingestion_ts
    from {{ source('bronze', 'coingecko_market_data') }}
    where run_key = '{{ run_key }}'
      and (
        ath_date > current_timestamp()
        or atl_date > current_timestamp()
        or last_updated > current_timestamp()
        or ingestion_ts > current_timestamp()
      )
)

select *
from invalid_timestamps
