{{ config(materialized='view', schema='bronze') }}

select
    value,
    value_classification,
    metric_date,
    time_until_update,
    ingestion_ts,
    source,
    run_key
from {{ source('bronze', 'fear_greed_raw') }}
