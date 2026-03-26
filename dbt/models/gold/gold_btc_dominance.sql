{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='metric_date',
    on_schema_change='sync_all_columns'
) }}

{% set gold_reprocess_days = var('gold_reprocess_days', 90) %}

-- Bitcoin dominance and relative performance vs altcoins.
-- Enables: "How does BTC dominance shift during fear/greed extremes?"
--          "How do altcoins perform relative to BTC?"
--          "Does BTC lead or follow altcoin price moves?"

with market_totals as (
    select
        metric_date,
        sum(case when market_cap is not null then market_cap else 0 end) as total_market_cap,
        sum(total_volume) as total_market_volume,
        count(distinct id) as num_coins,
        sum(case when market_cap is null then 1 else 0 end) as unknown_market_cap_asset_count,
        sum(case when market_cap is not null then 1 else 0 end) as known_market_cap_asset_count
        from {{ ref('dim_crypto_daily') }}
    where true
            {% if is_incremental() %}
            and metric_date >= (
                        select date_sub(coalesce(max(metric_date), current_date), {{ gold_reprocess_days }})
                        from {{ this }}
            )
            {% endif %}
    group by metric_date
),

btc as (
    select
        metric_date,
        current_price as btc_price,
        market_cap as btc_market_cap,
        total_volume as btc_volume,
        price_change_percentage_24h as btc_price_change_pct_24h,
        fear_greed_value,
        fear_greed_label,
        btc_tx_count,
        btc_unique_addresses,
        btc_hash_rate,
        btc_mempool_size
    from {{ ref('dim_crypto_daily') }}
    where id = 'bitcoin'
        {% if is_incremental() %}
        and metric_date >= (
            select date_sub(coalesce(max(metric_date), current_date), {{ gold_reprocess_days }})
            from {{ this }}
        )
        {% endif %}
),

eth as (
    select
        metric_date,
        current_price as eth_price,
        market_cap as eth_market_cap,
        total_volume as eth_volume,
        price_change_percentage_24h as eth_price_change_pct_24h
    from {{ ref('dim_crypto_daily') }}
    where id = 'ethereum'
        {% if is_incremental() %}
        and metric_date >= (
            select date_sub(coalesce(max(metric_date), current_date), {{ gold_reprocess_days }})
            from {{ this }}
        )
        {% endif %}
),

btc_returns as (
    select
        metric_date,
        btc_price,
        lag(btc_price) over (order by metric_date) as prev_btc_price
    from btc
),

eth_returns as (
    select
        metric_date,
        eth_price,
        lag(eth_price) over (order by metric_date) as prev_eth_price
    from eth
)

select
    b.metric_date,
    b.btc_price,
    e.eth_price,
    b.btc_market_cap,
    e.eth_market_cap,
    t.total_market_cap,
    round(b.btc_market_cap / t.total_market_cap * 100, 2) as btc_dominance_pct,
    round(e.eth_market_cap / t.total_market_cap * 100, 2) as eth_dominance_pct,
    round((t.total_market_cap - b.btc_market_cap) / t.total_market_cap * 100, 2) as altcoin_dominance_pct,
    b.btc_volume,
    e.eth_volume,
    t.total_market_volume,
    round(b.btc_volume / nullif(t.total_market_volume, 0) * 100, 2) as btc_volume_share_pct,
    case
        when br.prev_btc_price is not null and br.prev_btc_price > 0
        then round((b.btc_price - br.prev_btc_price) / br.prev_btc_price * 100, 4)
        else null
    end as btc_daily_return_pct,
    case
        when er.prev_eth_price is not null and er.prev_eth_price > 0
        then round((e.eth_price - er.prev_eth_price) / er.prev_eth_price * 100, 4)
        else null
    end as eth_daily_return_pct,
    case
        when e.eth_price is not null and e.eth_price > 0
        then round(b.btc_price / e.eth_price, 4)
        else null
    end as btc_eth_ratio,
    b.fear_greed_value,
    b.fear_greed_label,
    b.btc_tx_count,
    b.btc_unique_addresses,
    b.btc_hash_rate,
    b.btc_mempool_size,
    t.num_coins,
    t.unknown_market_cap_asset_count,
    t.known_market_cap_asset_count
from btc b
inner join market_totals t
    on b.metric_date = t.metric_date
left join eth e
    on b.metric_date = e.metric_date
left join btc_returns br
    on b.metric_date = br.metric_date
left join eth_returns er
    on b.metric_date = er.metric_date
where t.total_market_cap > 0
