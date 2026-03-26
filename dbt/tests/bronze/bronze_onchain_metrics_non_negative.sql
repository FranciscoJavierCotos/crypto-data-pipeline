with invalid_rows as (
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
    where source = 'blockchain_info_charts_api'
      and (
        metric_date > current_date()
        or (btc_tx_count is not null and btc_tx_count < 0)
        or (btc_unique_addresses is not null and btc_unique_addresses < 0)
        or (btc_hash_rate is not null and btc_hash_rate < 0)
        or (btc_mempool_size is not null and btc_mempool_size < 0)
      )
)

select *
from invalid_rows
