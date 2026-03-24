{{ config(materialized='table') }}

-- Placeholder scaffold for Gold model. Transformation logic will be added later.
select
    cast(null as string) as id,
    cast(null as date) as metric_date,
    cast(null as double) as volatility_signal,
    cast(null as string) as volatility_bucket
where 1 = 0