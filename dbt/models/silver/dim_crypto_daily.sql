{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['id', 'metric_date'],
    on_schema_change='sync_all_columns'
) }}

{% set silver_reprocess_days = var('silver_reprocess_days', 14) %}

with coingecko_daily as (
    select
        id,
        symbol,
        name,
        cast(last_updated as date) as metric_date,
        current_price,
        market_cap,
        total_volume,
        high_24h,
        low_24h,
        price_change_24h,
        price_change_percentage_24h,
        market_cap_rank,
        fully_diluted_valuation,
        circulating_supply,
        total_supply,
        max_supply,
        ath,
        ath_change_percentage,
        atl,
        atl_change_percentage,
        case
            when market_cap is not null and market_cap > 0
            then total_volume / market_cap
            else null
        end as volume_to_market_cap_ratio,
        ingestion_ts,
        row_number() over (
            partition by id, cast(last_updated as date)
            order by ingestion_ts desc, last_updated desc
        ) as row_num
    from {{ ref('stg_bronze_coingecko_market_data') }}
    where id is not null
      and last_updated is not null
        {% if is_incremental() %}
        and cast(last_updated as date) >= (
            select date_sub(coalesce(max(metric_date), current_date), {{ silver_reprocess_days }})
            from {{ this }}
        )
        {% endif %}
),

fear_greed_daily as (
    select
        metric_date,
        value as fear_greed_value,
        value_classification as fear_greed_label,
        ingestion_ts,
        row_number() over (
            partition by metric_date
            order by ingestion_ts desc
        ) as row_num
    from {{ ref('stg_bronze_fear_greed_raw') }}
    where metric_date is not null
        {% if is_incremental() %}
        and metric_date >= (
            select date_sub(coalesce(max(metric_date), current_date), {{ silver_reprocess_days }})
            from {{ this }}
        )
        {% endif %}
),

onchain_daily as (
    select
        metric_date,
        btc_tx_count,
        btc_unique_addresses,
        btc_hash_rate,
        btc_mempool_size,
        ingestion_ts,
        row_number() over (
            partition by metric_date
            order by ingestion_ts desc
        ) as row_num
    from {{ ref('stg_bronze_onchain_macro_raw') }}
    where metric_date is not null
        {% if is_incremental() %}
        and metric_date >= (
            select date_sub(coalesce(max(metric_date), current_date), {{ silver_reprocess_days }})
            from {{ this }}
        )
        {% endif %}
)

select
    c.id,
    c.symbol,
    c.name,
    c.metric_date,
    c.current_price,
    c.market_cap,
    c.total_volume,
    c.high_24h,
    c.low_24h,
    c.price_change_24h,
    c.price_change_percentage_24h,
    c.market_cap_rank,
    c.fully_diluted_valuation,
    c.circulating_supply,
    c.total_supply,
    c.max_supply,
    c.ath,
    c.ath_change_percentage,
    c.atl,
    c.atl_change_percentage,
    c.volume_to_market_cap_ratio,
    f.fear_greed_value,
    f.fear_greed_label,
    case when f.metric_date is not null then true else false end as has_fear_greed_data,
    o.btc_tx_count,
    o.btc_unique_addresses,
    o.btc_hash_rate,
    o.btc_mempool_size,
    case when o.metric_date is not null then true else false end as has_onchain_data,
    greatest(
        c.ingestion_ts,
        coalesce(f.ingestion_ts, c.ingestion_ts),
        coalesce(o.ingestion_ts, c.ingestion_ts)
    ) as ingestion_ts
from coingecko_daily c
left join fear_greed_daily f
    on c.metric_date = f.metric_date
   and f.row_num = 1
left join onchain_daily o
    on c.metric_date = o.metric_date
   and o.row_num = 1
where c.row_num = 1