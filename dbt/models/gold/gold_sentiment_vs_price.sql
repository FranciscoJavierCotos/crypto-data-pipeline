{{ config(materialized='table') }}

-- Placeholder scaffold for Gold model. Transformation logic will be added later.
select
    cast(null as string) as id,
    cast(null as date) as metric_date,
    cast(null as int) as fear_greed_value,
    cast(null as string) as fear_greed_label,
    cast(null as double) as next_day_price_change_pct
where 1 = 0