{{ config(materialized='view', schema='bronze') }}

select
    metric_date,
    btc_tx_count,
    btc_unique_addresses,
    btc_hash_rate,
    btc_mempool_size,
    ingestion_ts,
    source,
    run_key
from {{ source('bronze', 'onchain_macro_raw') }}
