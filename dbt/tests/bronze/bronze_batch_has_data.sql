{% set run_key = var('run_key', '') %}

with batch_stats as (
    select
        '{{ run_key }}' as expected_run_key,
        count(*) as row_count
    from {{ source('bronze', 'coingecko_market_data') }}
    where run_key = '{{ run_key }}'
)

select *
from batch_stats
where row_count = 0
