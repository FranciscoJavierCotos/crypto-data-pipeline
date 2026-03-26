{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['id', 'metric_date'],
    on_schema_change='append_new_columns'
) }}

{% set gold_reprocess_days = var('gold_reprocess_days', 90) %}

-- Liquidity ranking across all crypto assets per day.
-- Ranks coins by volume-to-market-cap ratio and flags anomalies.
-- Enables: "Which coins are most/least liquid?" and "Is BTC more liquid than altcoins?"

with daily_liquidity as (
    select
        id,
        symbol,
        name,
        metric_date,
        current_price,
        market_cap,
        total_volume,
        volume_to_market_cap_ratio,
        market_cap_rank,
        avg(volume_to_market_cap_ratio) over (
            partition by id
            order by metric_date
            rows between 6 preceding and current row
        ) as avg_liquidity_7d,
        avg(volume_to_market_cap_ratio) over (
            partition by id
            order by metric_date
            rows between 29 preceding and current row
        ) as avg_liquidity_30d,
        stddev_samp(volume_to_market_cap_ratio) over (
            partition by id
            order by metric_date
            rows between 29 preceding and current row
        ) as std_liquidity_30d
        from {{ ref('dim_crypto_daily') }}
    where volume_to_market_cap_ratio is not null
            {% if is_incremental() %}
            and metric_date >= (
                        select date_sub(coalesce(max(metric_date), current_date), {{ gold_reprocess_days }})
                        from {{ this }}
            )
            {% endif %}
),

ranked as (
    select
        *,
        rank() over (
            partition by metric_date
            order by volume_to_market_cap_ratio desc
        ) as liquidity_rank,
        rank() over (
            partition by metric_date
            order by volume_to_market_cap_ratio asc
        ) as illiquidity_rank
    from daily_liquidity
)

select
    id,
    symbol,
    name,
    metric_date,
    current_price,
    market_cap,
    total_volume,
    round(volume_to_market_cap_ratio, 6) as volume_to_market_cap_ratio,
    round(avg_liquidity_7d, 6) as avg_liquidity_7d,
    round(avg_liquidity_30d, 6) as avg_liquidity_30d,
    liquidity_rank,
    case
        when std_liquidity_30d is not null
             and std_liquidity_30d > 0
             and volume_to_market_cap_ratio > avg_liquidity_30d + 2 * std_liquidity_30d
        then true
        else false
    end as high_volume_anomaly_flag,
    case
        when market_cap_rank <= 5 and liquidity_rank > 5 then true
        else false
    end as high_cap_low_liquidity_flag
from ranked