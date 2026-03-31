{{ config(
    materialized='table',
    schema='gold',
    meta={
        'owner': 'data-engineering',
        'layer': 'gold'
    },
    tags=['gold', 'analytics', 'market_summary']
) }}

{% set gold_reprocess_days = var('gold_reprocess_days', 90) %}
{% set gold_min_coverage_pct = var('gold_min_coverage_pct', 70) %}
{% set dim_daily_relation = ref('dim_crypto_daily') %}
{% set dim_daily_columns = adapter.get_columns_in_relation(dim_daily_relation) %}
{% set dim_daily_column_names = dim_daily_columns | map(attribute='name') | map('lower') | list %}

-- Daily market summary: one row per coin per day with all key metrics.
-- Designed as the "go-to" table for ad-hoc analysis about Bitcoin vs altcoins.
-- Enables: "Compare BTC and ETH on any given day across price, volume, sentiment, on-chain"

with base as (
    select
        id,
        symbol,
        name,
        metric_date,
        current_price,
        market_cap,
        total_volume,
        high_24h,
        low_24h,
        price_change_percentage_24h,
        market_cap_rank,
        circulating_supply,
        volume_to_market_cap_ratio,
        ath,
        ath_change_percentage,
        atl,
        atl_change_percentage,
        fear_greed_value,
        fear_greed_label,
        case when fear_greed_value is not null then true else false end as has_fear_greed_data,
        btc_tx_count,
        btc_unique_addresses,
        btc_hash_rate,
        btc_mempool_size,
                {% if 'has_onchain_data' in dim_daily_column_names %}
                has_onchain_data,
                {% else %}
                case
                        when btc_tx_count is not null
                            or btc_unique_addresses is not null
                            or btc_hash_rate is not null
                            or btc_mempool_size is not null
                        then true else false
                end as has_onchain_data,
                {% endif %}
        lag(current_price) over (
            partition by id order by metric_date
        ) as prev_day_price
        from {{ dim_daily_relation }}
        where true
            {% if is_incremental() %}
            and metric_date >= (
                        select date_sub(coalesce(max(metric_date), current_date), {{ gold_reprocess_days }})
                        from {{ this }}
            )
            {% endif %}
),

btc_price as (
    select metric_date, current_price as btc_price
    from {{ dim_daily_relation }}
    where id = 'bitcoin'
    {% if is_incremental() %}
    and metric_date >= (
        select date_sub(coalesce(max(metric_date), current_date), {{ gold_reprocess_days }})
        from {{ this }}
    )
    {% endif %}
)

select
    b.id,
    b.symbol,
    b.name,
    b.metric_date,
    b.current_price,
    b.market_cap,
    b.total_volume,
    b.high_24h,
    b.low_24h,
    b.market_cap_rank,
    b.circulating_supply,
    round(b.volume_to_market_cap_ratio, 6) as volume_to_market_cap_ratio,

    -- Daily return
    case
        when b.prev_day_price is not null and b.prev_day_price > 0
        then round((b.current_price - b.prev_day_price) / b.prev_day_price * 100, 4)
        else null
    end as daily_return_pct,

    -- Intraday range
    case
        when b.low_24h is not null and b.low_24h > 0
        then round((b.high_24h - b.low_24h) / b.low_24h * 100, 4)
        else null
    end as intraday_range_pct,

    -- Distance from ATH/ATL
    round(b.ath_change_percentage, 2) as distance_from_ath_pct,
    round(b.atl_change_percentage, 2) as distance_from_atl_pct,

    -- Price relative to BTC (BTC-denominated price)
    case
        when bp.btc_price is not null and bp.btc_price > 0
        then round(b.current_price / bp.btc_price, 8)
        else null
    end as price_in_btc,

    -- Sentiment
    b.fear_greed_value,
    b.fear_greed_label,
    b.has_fear_greed_data,

    -- On-chain (same for all coins, BTC-specific)
    b.btc_tx_count,
    b.btc_unique_addresses,
    b.btc_hash_rate,
    b.btc_mempool_size,
    b.has_onchain_data,

    -- Row-level quality score from silver context availability and core market fields.
    round((
        (case when b.has_fear_greed_data then 1 else 0 end)
        + (case when b.has_onchain_data then 1 else 0 end)
        + (case when b.market_cap is not null then 1 else 0 end)
        + (case when b.total_volume is not null then 1 else 0 end)
    ) / 4.0 * 100, 2) as data_completeness_score_pct

from base b
left join btc_price bp
    on b.metric_date = bp.metric_date
where (
    (case when b.market_cap is not null then 1 else 0 end)
    + (case when b.total_volume is not null then 1 else 0 end)
    + (case when b.has_fear_greed_data then 1 else 0 end)
    + (case when b.has_onchain_data then 1 else 0 end)
) / 4.0 * 100 >= {{ gold_min_coverage_pct }}
