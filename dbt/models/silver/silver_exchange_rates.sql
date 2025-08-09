{{ config(materialized='table') }}

-- Silver exchange rates - direct passthrough from staging
select
    date,
    from_currency,
    to_currency,
    exchange_rate,
    rate_source
from {{ ref('stg_exchange_rates') }}
