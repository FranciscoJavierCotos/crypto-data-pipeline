{{ config(materialized='view', schema='bronze') }}

select
    id,
    symbol,
    name,
    current_price,
    market_cap,
    total_volume,
    high_24h,
    low_24h,
    last_updated,
    ingestion_ts,
    source,
    run_key
from {{ source('bronze', 'coingecko_market_data') }}
