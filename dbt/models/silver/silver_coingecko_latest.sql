with bronze_source as (
    select *
    from {{ source('bronze', 'coingecko_market_data') }}
),
ranked as (
    select
        id,
        symbol,
        name,
        current_price,
        market_cap,
        total_volume,
        high_24h,
        low_24h,
        price_change_24h,
        price_change_percentage_24h,
        last_updated,
        ingestion_ts,
        source,
        row_number() over (
            partition by id
            order by ingestion_ts desc, last_updated desc
        ) as row_num
    from bronze_source
)

select
    id,
    symbol,
    name,
    current_price,
    market_cap,
    total_volume,
    high_24h,
    low_24h,
    price_change_24h,
    price_change_percentage_24h,
    last_updated,
    ingestion_ts,
    source
from ranked
where row_num = 1
