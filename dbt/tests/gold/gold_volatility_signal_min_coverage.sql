-- Gold test: volatility table key columns must meet minimum coverage threshold
-- A passing test returns 0 rows.

{% set min_coverage_pct = var('gold_min_coverage_pct', 70) | float %}

with stats as (
    select
        count(*) as total_rows,
        sum(case when daily_return_pct is not null then 1 else 0 end) as daily_return_rows,
        sum(case when volatility_bucket is not null then 1 else 0 end) as volatility_bucket_rows
    from {{ ref('gold_volatility_signal') }}
)

select *
from stats
where total_rows > 0
  and (
      daily_return_rows * 100.0 / total_rows < {{ min_coverage_pct }}
    or volatility_bucket_rows * 100.0 / total_rows < {{ min_coverage_pct }}
  )
