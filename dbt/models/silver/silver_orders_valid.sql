{{ config(materialized='table') }}

-- Silver orders - clean, valid records only (quality issues filtered out)
with flagged_orders as (
    select * from {{ ref('stg_orders_with_flags') }}
)

select 
    order_id,
    line_number,
    sku_id,
    item_description_original,
    item_description_cleaned,
    quantity,
    unit_price,
    currency,
    vendor_brand,
    cost_centre_id,
    country_code,
    requisitioner,
    approval_status,
    delivery_date,
    order_date,
    source_filename,
    created_timestamp,
    non_product_hint,
    line_uid,
    
    -- Vendor matching details (for valid records)
    matched_vendor_id,
    matched_vendor_name,
    vendor_fuzz_score
    
from flagged_orders
where 
    coalesce(flag_missing_in_seed, false) = false
    and coalesce(flag_name_mismatch, false) = false
    and coalesce(flag_vendor_mismatch, false) = false
