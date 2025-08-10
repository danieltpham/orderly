{{ config(materialized='table') }}

-- Data quality monitoring and exception tracking fact table
-- Grain: One row per exception record
with exception_records as (
    select
        order_id,
        line_number,
        sku_id,
        cost_centre_id,
        matched_vendor_id,
        order_date,
        
        -- Quality flags and scores
        flag_missing_in_seed,
        flag_name_mismatch,
        flag_vendor_mismatch,
        vendor_fuzz_score,
        fuzz_score,
        exception_type,
        
        -- Additional context
        vendor_brand_original,
        matched_vendor_name,
        item_description_original,
        item_description_cleaned,
        line_uid
        
    from {{ ref('silver_orders_exceptions') }}
),

-- Join with dimensions to get surrogate keys
fact_with_keys as (
    select
        er.*,
        
        -- Date dimension key
        dd.date_key,
        
        -- Dimension keys via lookups
        dc.cost_centre_key,
        dv.vendor_key,
        dp.product_key
        
    from exception_records er
    
    -- Join order date
    left join {{ ref('dim_date') }} dd 
        on dd.date_actual = er.order_date
    
    -- Join cost centre dimension
    left join {{ ref('dim_cost_centre') }} dc 
        on dc.cost_centre_id = er.cost_centre_id
    
    -- Join vendor dimension (may be null for vendor exceptions)
    left join {{ ref('dim_vendor') }} dv 
        on dv.vendor_id = er.matched_vendor_id
    
    -- Join product dimension (may be null for SKU exceptions)
    left join {{ ref('dim_product') }} dp 
        on dp.sku_id = er.sku_id
),

-- Final fact table
final_fact as (
    select
        -- Surrogate fact key
        md5(concat(order_id, '|', line_number, '|', exception_type)) as data_quality_key,
        
        -- Dimension keys
        date_key,
        cost_centre_key,
        vendor_key,  -- May be null for vendor exceptions
        product_key, -- May be null for SKU exceptions
        
        -- Measures (semi-additive - count exceptions)
        1 as exception_count,
        coalesce(fuzz_score, 0) as fuzz_score,
        coalesce(vendor_fuzz_score, 0) as vendor_fuzz_score,
        
        -- Quality dimensions/attributes
        exception_type,
        flag_missing_in_seed,
        flag_name_mismatch,
        flag_vendor_mismatch,
        
        -- Degenerate dimensions (detailed attributes)
        order_id,
        line_number,
        vendor_brand_original,
        matched_vendor_name,
        item_description_original,
        item_description_cleaned,
        
        -- Metadata
        line_uid,
        
        -- Audit fields
        current_timestamp as created_at
        
    from fact_with_keys
    where date_key is not null  -- Ensure we have valid date
      and cost_centre_key is not null  -- Ensure valid cost centre
)

select * from final_fact
order by date_key desc, exception_type, order_id, line_number
