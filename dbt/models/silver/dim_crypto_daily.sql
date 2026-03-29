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
),

fear_greed_latest as (
    select
        metric_date,
        fear_greed_value,
        fear_greed_label,
        ingestion_ts
    from fear_greed_daily
    where row_num = 1
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
),

onchain_latest as (
    select
        metric_date,
        btc_tx_count,
        btc_unique_addresses,
        btc_hash_rate,
        btc_mempool_size,
        ingestion_ts
    from onchain_daily
    where row_num = 1
),

coins as (
    select *
    from coingecko_daily
    where row_num = 1
),

date_spine as (
    select distinct metric_date
    from coins
    union
    select distinct metric_date
    from fear_greed_latest
    union
    select distinct metric_date
    from onchain_latest
),

fear_greed_asof as (
    select
        d.metric_date,
        last_value(f.fear_greed_value, true) over (
            order by d.metric_date
            rows between unbounded preceding and current row
        ) as fear_greed_value,
        last_value(f.fear_greed_label, true) over (
            order by d.metric_date
            rows between unbounded preceding and current row
        ) as fear_greed_label,
        last_value(f.ingestion_ts, true) over (
            order by d.metric_date
            rows between unbounded preceding and current row
        ) as ingestion_ts
    from date_spine d
    left join fear_greed_latest f
        on d.metric_date = f.metric_date
),

onchain_asof as (
    select
        d.metric_date,
        last_value(o.btc_tx_count, true) over (
            order by d.metric_date
            rows between unbounded preceding and current row
        ) as btc_tx_count,
        last_value(o.btc_unique_addresses, true) over (
            order by d.metric_date
            rows between unbounded preceding and current row
        ) as btc_unique_addresses,
        last_value(o.btc_hash_rate, true) over (
            order by d.metric_date
            rows between unbounded preceding and current row
        ) as btc_hash_rate,
        last_value(o.btc_mempool_size, true) over (
            order by d.metric_date
            rows between unbounded preceding and current row
        ) as btc_mempool_size,
        last_value(o.ingestion_ts, true) over (
            order by d.metric_date
            rows between unbounded preceding and current row
        ) as ingestion_ts
    from date_spine d
    left join onchain_latest o
        on d.metric_date = o.metric_date
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
    case when f.fear_greed_value is not null then true else false end as has_fear_greed_data,
    o.btc_tx_count,
    o.btc_unique_addresses,
    o.btc_hash_rate,
    o.btc_mempool_size,
    case
        when o.btc_tx_count is not null
            or o.btc_unique_addresses is not null
            or o.btc_hash_rate is not null
            or o.btc_mempool_size is not null
        then true
        else false
    end as has_onchain_data,
    greatest(
        c.ingestion_ts,
        coalesce(f.ingestion_ts, c.ingestion_ts),
        coalesce(o.ingestion_ts, c.ingestion_ts)
    ) as ingestion_ts
from coins c
left join fear_greed_asof f
    on c.metric_date = f.metric_date
left join onchain_asof o
    on c.metric_date = o.metric_date
