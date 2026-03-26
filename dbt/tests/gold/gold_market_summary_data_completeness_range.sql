-- Gold test: data_completeness_score_pct should be within 0..100
-- A passing test returns 0 rows.

select
    id,
    metric_date,
    data_completeness_score_pct
from {{ ref('gold_market_summary') }}
where data_completeness_score_pct < 0
   or data_completeness_score_pct > 100
