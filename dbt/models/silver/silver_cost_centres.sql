{{ config(materialized='table') }}

-- Silver cost centres - direct passthrough from bronze
select
    cost_centre_id,
    cost_centre_name,
    country_code
from {{ ref('raw_cost_centres') }}
