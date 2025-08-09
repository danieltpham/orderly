{{ config(materialized='table') }}

-- Silver vendor master - direct passthrough from bronze
select
    cast(vendor_id as varchar) as vendor_id,
    vendor_name
from {{ ref('raw_vendor_master') }}
