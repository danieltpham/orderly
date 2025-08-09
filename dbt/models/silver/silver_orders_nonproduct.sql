{{ config(materialized='table') }}

-- Silver orders non-product - records identified as non-product items (services, fees, etc.)
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
    
    -- Quality flags
    flag_missing_in_seed,
    flag_name_mismatch,
    has_quality_issues,
    
    -- Fuzzy comparison details (for debugging/analysis)
    fuzz_score,
    compared_order_desc,
    compared_seed_name,
    
    -- Non-product categorization
    'NON_PRODUCT' as record_type
from flagged_orders
where 
    non_product_hint = true
    and sku_id is null