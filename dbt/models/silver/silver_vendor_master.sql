{{ config(materialized='table') }}

-- Silver vendor master - direct passthrough from bronze
select * from {{ ref('raw_vendor_master') }}
