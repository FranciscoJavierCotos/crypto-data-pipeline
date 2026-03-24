{{ config(materialized='table') }}

-- Placeholder scaffold for Gold model. Transformation logic will be added later.
select
    cast(null as string) as id,
    cast(null as date) as metric_date,
    cast(null as double) as volume_to_market_cap_ratio,
    cast(null as int) as liquidity_rank,
    cast(null as boolean) as high_cap_anomaly_flag
where 1 = 0