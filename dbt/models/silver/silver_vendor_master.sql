{{ config(materialized='table') }}

-- Silver vendor master - direct passthrough from bronze
select
    vendor_id,
    vendor_name
from {{ ref('raw_vendor_master') }}
