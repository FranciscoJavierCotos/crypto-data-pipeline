select
    current_date() as snapshot_date,
    count(*) as active_coins,
    sum(market_cap) as total_market_cap,
    avg(current_price) as avg_current_price,
    sum(total_volume) as total_volume_24h,
    max(ingestion_ts) as latest_ingestion_ts
from {{ ref('silver_coingecko_latest') }}
