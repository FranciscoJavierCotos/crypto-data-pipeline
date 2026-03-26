{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['id', 'metric_date'],
    on_schema_change='sync_all_columns'
) }}

{% set gold_reprocess_days = var('gold_reprocess_days', 90) %}

with base as (
    select
        id,
        metric_date,
        current_price,
        total_volume,
        volume_to_market_cap_ratio,
        fear_greed_value,
        fear_greed_label,
        btc_tx_count,
        btc_unique_addresses,
        btc_hash_rate,
        btc_mempool_size,
        lead(current_price) over (
            partition by id
            order by metric_date
        ) as next_day_price
        from {{ ref('dim_crypto_daily') }}
        where true
            {% if is_incremental() %}
            and metric_date >= (
                        select date_sub(coalesce(max(metric_date), current_date), {{ gold_reprocess_days }})
                        from {{ this }}
            )
            {% endif %}
),

date_points as (
    select distinct metric_date, btc_tx_count
    from base
    where btc_tx_count is not null
),

date_regime as (
    select
        metric_date,
        btc_tx_count,
        avg(btc_tx_count) over (
            order by metric_date
            rows between 29 preceding and current row
        ) as avg_tx_30d,
        stddev_samp(btc_tx_count) over (
            order by metric_date
            rows between 29 preceding and current row
        ) as std_tx_30d,
        count(btc_tx_count) over (
            order by metric_date
            rows between 29 preceding and current row
        ) as obs_tx_30d
    from date_points
)

select
    b.id,
    b.metric_date,
    b.fear_greed_value,
    b.fear_greed_label,
    b.btc_tx_count,
    b.btc_unique_addresses,
    b.btc_hash_rate,
    b.btc_mempool_size,
    b.current_price,
    b.total_volume,
    b.volume_to_market_cap_ratio,
    case
        when b.current_price is null or b.current_price = 0 or b.next_day_price is null then null
        else ((b.next_day_price - b.current_price) / b.current_price) * 100
    end as next_day_price_change_pct,
    case
        when r.obs_tx_30d < 10 or r.std_tx_30d is null or r.std_tx_30d = 0 then 'normal_onchain_activity'
        when r.btc_tx_count >= r.avg_tx_30d + r.std_tx_30d then 'high_onchain_activity'
        when r.btc_tx_count <= r.avg_tx_30d - r.std_tx_30d then 'low_onchain_activity'
        else 'normal_onchain_activity'
    end as onchain_activity_regime
from base b
left join date_regime r
    on b.metric_date = r.metric_date