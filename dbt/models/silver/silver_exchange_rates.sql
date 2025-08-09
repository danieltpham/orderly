{{ config(materialized='table') }}

-- Silver exchange rates - direct passthrough from bronze
select
    date,
    from_currency,
    to_currency,
    exchange_rate
from {{ ref('raw_exchange_rates') }}
