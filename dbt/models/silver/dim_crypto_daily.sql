{{ config(materialized='table') }}

-- Placeholder scaffold for Silver model. Transformation logic will be added later.
select
    cast(null as string) as id,
    cast(null as date) as metric_date,
    cast(null as int) as market_cap_rank,
    cast(null as double) as circulating_supply,
    cast(null as double) as volume_to_market_cap_ratio,
    cast(null as int) as fear_greed_value,
    cast(null as string) as fear_greed_label
where 1 = 0