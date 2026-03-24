with duplicated as (
    select
        metric_date,
        count(*) as duplicate_count
    from {{ source('bronze', 'fear_greed_raw') }}
    group by metric_date
    having count(*) > 1
)

select *
from duplicated
