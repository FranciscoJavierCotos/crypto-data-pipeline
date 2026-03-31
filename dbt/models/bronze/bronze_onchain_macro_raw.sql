-- ---------------------------------------------------------------------------
-- WHAT CHANGED (vs stg_bronze_onchain_macro_raw.sql):
--   1. Renamed from stg_bronze_ to bronze_ prefix.
--   2. Changed from 'view' to 'incremental' with merge on metric_date.
--   3. Added meta, tags, _loaded_at.
-- ---------------------------------------------------------------------------

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='metric_date',
    on_schema_change='append_new_columns',
    schema='bronze',
    meta={
        'owner': 'data-engineering',
        'layer': 'bronze',
        'source': 'blockchain_info_charts_api'
    },
    tags=['bronze', 'raw', 'onchain']
) }}

select
    metric_date,
    btc_tx_count,
    btc_unique_addresses,
    btc_hash_rate,
    btc_mempool_size,
    ingestion_ts,
    source,
    run_key,
    ingestion_ts as _loaded_at
from {{ source('bronze', 'onchain_macro_raw') }}
{% if is_incremental() %}
where ingestion_ts > (select max(ingestion_ts) from {{ this }})
{% endif %}
