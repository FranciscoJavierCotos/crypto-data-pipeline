-- Silver test: Ensure Bitcoin is present on every date that has data
-- If BTC is missing from a date that has other coins, something went wrong.
-- A passing test returns 0 rows.

with all_dates as (
    select distinct metric_date
    from {{ ref('dim_crypto_daily') }}
),

btc_dates as (
    select distinct metric_date
    from {{ ref('dim_crypto_daily') }}
    where id = 'bitcoin'
)

select d.metric_date
from all_dates d
left join btc_dates b on d.metric_date = b.metric_date
where b.metric_date is null
