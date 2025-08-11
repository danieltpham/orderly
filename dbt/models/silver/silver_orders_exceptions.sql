{{ config(materialized='table') }}

-- Silver orders exceptions - records with data quality issues for monitoring and analysis
-- Note: Non-product records are handled separately in silver_orders_nonproduct
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
    flag_vendor_mismatch,
    has_quality_issues,
    
    -- Fuzzy comparison details (for debugging/analysis)
    fuzz_score,
    compared_order_desc,
    compared_seed_name,
    
    -- Vendor matching details
    vendor_brand_original,
    cast(matched_vendor_id as varchar) as matched_vendor_id,
    matched_vendor_name,
    vendor_fuzz_score,
    
    -- Exception categorization (excluding non-product items)
    case 
        when coalesce(flag_missing_in_seed, false) then 'SKU_NOT_IN_SEED'
        when coalesce(flag_name_mismatch, false) then 'NAME_MISMATCH'
        when coalesce(flag_vendor_mismatch, false) then 'VENDOR_MISMATCH'
        else 'OTHER'
    end as exception_type   
from flagged_orders
where 
    -- Exclude non-product records (handled in silver_orders_nonproduct)
    not (non_product_hint = true and sku_id is null)
    and (
        coalesce(flag_missing_in_seed, false) = true
        or coalesce(flag_name_mismatch, false) = true
        or coalesce(flag_vendor_mismatch, false) = true
    )
