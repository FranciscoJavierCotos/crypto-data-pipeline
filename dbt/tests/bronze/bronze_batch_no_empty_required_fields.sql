{% set run_key = var('run_key', '') %}

with invalid_rows as (
    select
    run_key,
        id,
        symbol,
        name,
        source,
        last_updated,
        ingestion_ts
    from {{ source('bronze', 'coingecko_market_data') }}
    where run_key = '{{ run_key }}'
      and (
        trim(coalesce(run_key, '')) = ''
        or trim(coalesce(id, '')) = ''
        or trim(coalesce(symbol, '')) = ''
        or trim(coalesce(name, '')) = ''
        or trim(coalesce(source, '')) = ''
        or last_updated is null
        or ingestion_ts is null
      )
)

select *
from invalid_rows
