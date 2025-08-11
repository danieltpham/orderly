{{ config(materialized='table') }}

-- Core transactional fact table for order line analysis
-- Grain: One row per order line item
with order_lines as (
    select
        order_id,
        line_number,
        sku_id,
        cost_centre_id,
        coalesce(cast(matched_vendor_id as varchar), '')  as matched_vendor_id,
        quantity,
        unit_price,
        currency,
        order_date,
        delivery_date,
        requisitioner,
        approval_status,
        source_filename,
        line_uid,
        item_description_cleaned,
        
        -- Calculate line total in original currency
        quantity * unit_price as line_total_amount
        
    from {{ ref('silver_orders_valid') }}
    -- where matched_vendor_id is not null  -- Only include orders with valid vendor matches
),

-- Join with dimensions to get surrogate keys
fact_with_keys as (
    select
        ol.*,
        
        -- Date dimension keys
        dd_order.date_key as date_key,
        dd_delivery.date_key as delivery_date_key,
        
        -- Dimension keys via lookups
        dc.cost_centre_key,
        dv.vendor_key,
        dp.product_key,
        
        -- Exchange rate for conversion (from date dimension)
        dd_order.usd_to_aud,
        
        -- Currency key (simplified - could be enhanced with dim_currency later)
        case 
            when ol.currency = 'USD' then 1
            when ol.currency = 'AUD' then 2
            else 0  -- Unknown currency
        end as currency_key
        
    from order_lines ol
    
    -- Join order date
    left join {{ ref('dim_date') }} dd_order 
        on dd_order.date_actual = ol.order_date
    
    -- Join delivery date (optional)
    left join {{ ref('dim_date') }} dd_delivery 
        on dd_delivery.date_actual = ol.delivery_date
    
    -- Join cost centre dimension
    left join {{ ref('dim_cost_centre') }} dc 
        on dc.cost_centre_id = ol.cost_centre_id
    
    -- Join vendor dimension  
    left join {{ ref('dim_vendor') }} dv 
        on dv.vendor_id = ol.matched_vendor_id
    
    -- Join product dimension
    left join {{ ref('dim_product') }} dp 
        on dp.sku_id = ol.sku_id
),

-- Final fact table with currency conversions
final_fact as (
    select
        -- Surrogate fact key
        md5(concat(order_id, '|', line_number)) as order_line_key,
        
        -- Dimension keys
        date_key,
        delivery_date_key,
        cost_centre_key,
        vendor_key,
        product_key,
        currency_key,
        
        -- Measures (additive)
        quantity,
        unit_price,
        line_total_amount,
        
        -- Currency converted measures
        case 
            when currency = 'USD' then line_total_amount * usd_to_aud
            when currency = 'AUD' then line_total_amount
            else null
        end as line_total_aud,
        
        case 
            when currency = 'USD' then line_total_amount
            when currency = 'AUD' then line_total_amount / usd_to_aud
            else null
        end as line_total_usd,
        
        -- Degenerate dimensions (attributes stored in fact)
        order_id,
        line_number,
        requisitioner,
        approval_status,
        source_filename,
        
        -- Metadata
        line_uid,
        currency as original_currency,
        
        -- Audit fields
        current_timestamp as created_at
        
    from fact_with_keys
    -- where date_key is not null  -- Ensure we have valid date
    --   and cost_centre_key is not null  -- Ensure valid cost centre
    --   and vendor_key is not null  -- Ensure valid vendor
    --   and product_key is not null  -- Ensure valid product
)

select * from final_fact
order by date_key desc, order_id, line_number
