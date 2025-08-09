{{ config(materialized='table') }}

-- Silver orders with quality flags for SKU lookup issues
with 
staged_orders as (
    select * from {{ ref('stg_orders') }}
),

ref_sku_names as (
    select * from {{ ref('ref_sku_names') }}
),

fuzz_scores as (
    select * from {{ ref('stg_orders_fuzz_scores') }}
),

-- Flag 1: SKU appears in bronze/staged orders but not in seed
missing_in_seed as (
    select distinct so.line_uid
    from staged_orders so
    left join ref_sku_names rsn on rsn.sku_id = so.sku_id
    where rsn.sku_id is null
),

-- Combine all flags
flagged_orders as (
    select 
        so.*,
        
        -- Flag 1: Missing in seed
        case 
            when mis.line_uid is not null then true 
            else false 
        end as flag_missing_in_seed,
        
        -- Flag 2: Name mismatch (fuzzy score < 0.8)
        fs.fuzz_score,
        fs.order_description as compared_order_desc,
        fs.seed_name as compared_seed_name,
        case 
            when fs.fuzz_score is not null and fs.fuzz_score < 0.8 then true 
            else false 
        end as flag_name_mismatch,
        
        -- Overall quality flag
        case 
            when (mis.line_uid is not null) 
                or (fs.fuzz_score is not null and fs.fuzz_score < 0.8) 
            then true 
            else false 
        end as has_quality_issues
        
    from staged_orders so
    left join missing_in_seed mis on mis.line_uid = so.line_uid
    left join fuzz_scores fs on fs.line_uid = so.line_uid
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
    compared_seed_name
    
from flagged_orders
