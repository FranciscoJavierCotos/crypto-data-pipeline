-- Gold test: btc_dominance key columns must meet minimum coverage threshold
-- A passing test returns 0 rows.

{% set min_coverage_pct = var('gold_min_coverage_pct', 70) | float %}

with stats as (
    select
        count(*) as total_rows,
        sum(case when btc_daily_return_pct is not null then 1 else 0 end) as btc_return_rows,
        sum(case when eth_daily_return_pct is not null then 1 else 0 end) as eth_return_rows,
        sum(case when btc_tx_count is not null then 1 else 0 end) as btc_tx_rows,
        sum(case when btc_unique_addresses is not null then 1 else 0 end) as btc_unique_rows,
        sum(case when btc_hash_rate is not null then 1 else 0 end) as btc_hash_rows
    from {{ ref('gold_btc_dominance') }}
)

select *
from stats
where total_rows > 0
  and (
      btc_return_rows * 100.0 / total_rows < {{ min_coverage_pct }}
      or eth_return_rows * 100.0 / total_rows < {{ min_coverage_pct }}
      or btc_tx_rows * 100.0 / total_rows < {{ min_coverage_pct }}
      or btc_unique_rows * 100.0 / total_rows < {{ min_coverage_pct }}
      or btc_hash_rows * 100.0 / total_rows < {{ min_coverage_pct }}
  )
