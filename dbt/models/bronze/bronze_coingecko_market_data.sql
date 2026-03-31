-- ---------------------------------------------------------------------------
-- WHAT CHANGED (vs stg_bronze_coingecko_market_data.sql):
--   1. Renamed from stg_bronze_ to bronze_ prefix for consistency.
--   2. Changed materialization from 'view' to 'incremental' with merge.
--   3. Added unique_key, on_schema_change, meta, and tags.
--   4. Added _loaded_at lineage column.
--
-- DESIGN DECISION:
--   This creates a materialized copy of the source data. The source table
--   is written by Airflow directly; this model provides a dbt-managed
--   contract layer with schema enforcement and incremental tracking.
--   If storage cost is a concern, revert to materialized='view'.
-- ---------------------------------------------------------------------------

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['id', 'run_key'],
    on_schema_change='append_new_columns',
    schema='bronze',
    meta={
        'owner': 'data-engineering',
        'layer': 'bronze',
        'source': 'coingecko_api'
    },
    tags=['bronze', 'raw', 'coingecko']
) }}

select
    id,
    symbol,
    name,
    current_price,
    market_cap,
    total_volume,
    high_24h,
    low_24h,
    price_change_24h,
    price_change_percentage_24h,
    market_cap_rank,
    fully_diluted_valuation,
    market_cap_change_24h,
    market_cap_change_percentage_24h,
    circulating_supply,
    total_supply,
    max_supply,
    ath,
    ath_change_percentage,
    ath_date,
    atl,
    atl_change_percentage,
    atl_date,
    volume_to_market_cap_ratio,
    last_updated,
    ingestion_ts,
    source,
    run_key,
    ingestion_ts as _loaded_at
from {{ source('bronze', 'coingecko_market_data') }}
{% if is_incremental() %}
where ingestion_ts > (select max(ingestion_ts) from {{ this }})
{% endif %}
