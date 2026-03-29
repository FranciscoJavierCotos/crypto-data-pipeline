-- Gold test: sentiment_vs_price key columns must meet minimum coverage threshold
-- A passing test returns 0 rows.

{% set min_coverage_pct = var('gold_min_coverage_pct', 70) | float %}

with stats as (
    select
        count(*) as total_rows,
        sum(case when next_day_price_change_pct is not null then 1 else 0 end) as next_day_return_rows,
        sum(case when btc_tx_count is not null then 1 else 0 end) as btc_tx_rows,
        sum(case when btc_unique_addresses is not null then 1 else 0 end) as btc_unique_rows,
        sum(case when btc_hash_rate is not null then 1 else 0 end) as btc_hash_rows
    from {{ ref('gold_sentiment_vs_price') }}
)

select *
from stats
where total_rows > 0
  and (
      next_day_return_rows * 100.0 / total_rows < {{ min_coverage_pct }}
      or btc_tx_rows * 100.0 / total_rows < {{ min_coverage_pct }}
      or btc_unique_rows * 100.0 / total_rows < {{ min_coverage_pct }}
      or btc_hash_rows * 100.0 / total_rows < {{ min_coverage_pct }}
  )
