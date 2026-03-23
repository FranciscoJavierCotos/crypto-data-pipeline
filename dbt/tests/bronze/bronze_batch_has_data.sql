{% set batch_id = var('batch_id', '') %}

with batch_stats as (
    select
        '{{ batch_id }}' as expected_batch_id,
        count(*) as row_count
    from {{ source('bronze', 'coingecko_market_data') }}
    where batch_id = '{{ batch_id }}'
)

select *
from batch_stats
where row_count = 0
