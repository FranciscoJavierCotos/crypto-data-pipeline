-- Gold test: liquidity_ranking key columns must meet minimum coverage threshold
-- A passing test returns 0 rows.

{% set min_coverage_pct = var('gold_min_coverage_pct', 70) | float %}

with stats as (
    select
        count(*) as total_rows,
        sum(case when liquidity_rank is not null then 1 else 0 end) as liquidity_rank_rows,
        sum(case when avg_liquidity_7d is not null then 1 else 0 end) as avg_liquidity_7d_rows,
        sum(case when avg_liquidity_30d is not null then 1 else 0 end) as avg_liquidity_30d_rows
    from {{ ref('gold_liquidity_ranking') }}
)

select *
from stats
where total_rows > 0
  and (
      liquidity_rank_rows * 100.0 / total_rows < {{ min_coverage_pct }}
      or avg_liquidity_7d_rows * 100.0 / total_rows < {{ min_coverage_pct }}
      or avg_liquidity_30d_rows * 100.0 / total_rows < {{ min_coverage_pct }}
  )
