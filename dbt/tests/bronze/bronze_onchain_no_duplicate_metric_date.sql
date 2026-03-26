with duplicated as (
    select
        metric_date,
        count(*) as duplicate_count
    from {{ source('bronze', 'onchain_macro_raw') }}
    where source = 'blockchain_info_charts_api'
    group by metric_date
    having count(*) > 1
)

select *
from duplicated
