{% set batch_id = var('batch_id', '') %}

with duplicated as (
    select
        batch_id,
        id,
        count(*) as duplicate_count
    from {{ source('bronze', 'coingecko_market_data') }}
    where batch_id = '{{ batch_id }}'
    group by batch_id, id
    having count(*) > 1
)

select *
from duplicated
