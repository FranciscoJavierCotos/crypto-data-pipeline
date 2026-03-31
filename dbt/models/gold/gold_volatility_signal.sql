{{ config(
    materialized='table',
    schema='gold',
    meta={
        'owner': 'data-engineering',
        'layer': 'gold'
    },
    tags=['gold', 'analytics', 'volatility']
) }}

{% set gold_reprocess_days = var('gold_reprocess_days', 90) %}

-- Rolling volatility analysis per crypto asset.
-- Computes intraday range, daily returns, and 7d/30d rolling volatility.
-- Enables comparison of BTC volatility vs altcoins.

with daily_prices as (
    select
        id,
        symbol,
        name,
        metric_date,
        current_price,
        high_24h,
        low_24h,
        lag(current_price) over (
            partition by id order by metric_date
        ) as prev_day_price
        from {{ ref('dim_crypto_daily') }}
    where current_price is not null
      and current_price > 0
            {% if is_incremental() %}
            and metric_date >= (
                        select date_sub(coalesce(max(metric_date), current_date), {{ gold_reprocess_days }})
                        from {{ this }}
            )
            {% endif %}
),

with_returns as (
    select
        *,
        case
            when low_24h is not null and low_24h > 0
            then (high_24h - low_24h) / low_24h
            else null
        end as intraday_range_pct,
        case
            when prev_day_price is not null and prev_day_price > 0
            then (current_price - prev_day_price) / prev_day_price
            else null
        end as daily_return
    from daily_prices
),

rolling_vol as (
    select
        id,
        symbol,
        name,
        metric_date,
        current_price,
        high_24h,
        low_24h,
        intraday_range_pct,
        daily_return,
        stddev_samp(daily_return) over (
            partition by id
            order by metric_date
            rows between 6 preceding and current row
        ) as volatility_7d,
        stddev_samp(daily_return) over (
            partition by id
            order by metric_date
            rows between 29 preceding and current row
        ) as volatility_30d,
        count(daily_return) over (
            partition by id
            order by metric_date
            rows between 6 preceding and current row
        ) as obs_7d,
        count(daily_return) over (
            partition by id
            order by metric_date
            rows between 29 preceding and current row
        ) as obs_30d
    from with_returns
)

select
    id,
    symbol,
    name,
    metric_date,
    current_price,
    high_24h,
    low_24h,
    round(intraday_range_pct * 100, 4) as intraday_range_pct,
    round(daily_return * 100, 4) as daily_return_pct,
    case
        when obs_30d >= 20 and volatility_30d * 100 >= 5 then 'extreme'
        when obs_30d >= 20 and volatility_30d * 100 >= 3 then 'high'
        when obs_30d >= 20 and volatility_30d * 100 >= 1.5 then 'medium'
        when obs_30d >= 20 then 'low'
        when obs_7d >= 4 and volatility_7d * 100 >= 5 then 'extreme'
        when obs_7d >= 4 and volatility_7d * 100 >= 3 then 'high'
        when obs_7d >= 4 and volatility_7d * 100 >= 1.5 then 'medium'
        when obs_7d >= 4 then 'low'
        else 'insufficient_data'
    end as volatility_bucket
from rolling_vol
