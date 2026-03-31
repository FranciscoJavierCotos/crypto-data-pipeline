-- ---------------------------------------------------------------------------
-- WHAT CHANGED (vs stg_bronze_fear_greed_raw.sql):
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
        'source': 'alternative_me_api'
    },
    tags=['bronze', 'raw', 'fear_greed']
) }}

select
    value,
    value_classification,
    metric_date,
    time_until_update,
    ingestion_ts,
    source,
    run_key,
    ingestion_ts as _loaded_at
from {{ source('bronze', 'fear_greed_raw') }}
{% if is_incremental() %}
where ingestion_ts > (select max(ingestion_ts) from {{ this }})
{% endif %}
