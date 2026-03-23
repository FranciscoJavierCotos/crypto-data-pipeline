{% set batch_id = var('batch_id', '') %}

with invalid_rows as (
    select
        batch_id,
        id,
        symbol,
        name,
        source,
        last_updated,
        ingestion_ts
    from {{ source('bronze', 'coingecko_market_data') }}
    where batch_id = '{{ batch_id }}'
      and (
        trim(coalesce(batch_id, '')) = ''
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
