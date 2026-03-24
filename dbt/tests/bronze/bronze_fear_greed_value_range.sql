with invalid_values as (
    select
        metric_date,
        value,
        value_classification,
        ingestion_ts,
        run_key
    from {{ source('bronze', 'fear_greed_raw') }}
    where value < 0 or value > 100
)

select *
from invalid_values
