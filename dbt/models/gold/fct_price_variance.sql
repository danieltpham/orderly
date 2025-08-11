{{ config(materialized='table') }}

-- Price variance and anomaly detection fact table
-- Grain: One row per SKU per vendor per cost centre per time period with price variance
with base_orders as (
    select
        sku_id,
        matched_vendor_id as vendor_id,
        cost_centre_id,
        order_date,
        unit_price,
        currency,
        quantity,
        order_id,
        line_number,
        
        -- Convert to USD for consistent price analysis
        case 
            when currency = 'USD' then unit_price
            when currency = 'AUD' then unit_price / dd.usd_to_aud
            else null
        end as unit_price_usd
        
    from {{ ref('silver_orders_valid') }}
    left join {{ ref('dim_date') }} dd on dd.date_actual = order_date
    where matched_vendor_id is not null
      and unit_price > 0
      and quantity > 0
),

-- Aggregate by SKU/Vendor/Cost Centre/Date (daily grain)
daily_aggregates as (
    select
        sku_id,
        vendor_id,
        cost_centre_id,
        order_date,
        
        -- Price statistics
        avg(unit_price_usd) as avg_unit_price_usd,
        min(unit_price_usd) as min_unit_price_usd,
        max(unit_price_usd) as max_unit_price_usd,
        count(*) as order_line_count,
        sum(quantity) as total_quantity,
        
        -- Price variance calculations
        variance(unit_price_usd) as price_variance_usd,
        stddev(unit_price_usd) as price_stddev_usd
        
    from base_orders
    where unit_price_usd is not null
    group by sku_id, vendor_id, cost_centre_id, order_date
),

-- Calculate variance thresholds and identify anomalies
price_analysis as (
    select
        *,
        
        -- Price range analysis
        case 
            when max_unit_price_usd > 0 and min_unit_price_usd > 0 
            then (max_unit_price_usd - min_unit_price_usd) / min_unit_price_usd
            else 0
        end as price_range_ratio,
        
        -- Coefficient of variation (standardized measure of price variability)
        case 
            when avg_unit_price_usd > 0 and price_stddev_usd > 0
            then price_stddev_usd / avg_unit_price_usd
            else 0
        end as coefficient_of_variation,
        
        -- Flag significant variance (> 20% coefficient of variation or >50% range ratio)
        case 
            when (price_stddev_usd / nullif(avg_unit_price_usd, 0)) > 0.20 
              or ((max_unit_price_usd - min_unit_price_usd) / nullif(min_unit_price_usd, 0)) > 0.50
            then true
            else false
        end as has_significant_variance
        
    from daily_aggregates
),

-- Join with dimensions to get surrogate keys
fact_with_keys as (
    select
        pa.*,
        
        -- Dimension keys
        dd.date_key,
        dc.cost_centre_key,
        dv.vendor_key,
        dp.product_key
        
    from price_analysis pa
    
    -- Join dimensions
    left join {{ ref('dim_date') }} dd 
        on dd.date_actual = pa.order_date
    
    left join {{ ref('dim_cost_centre') }} dc 
        on dc.cost_centre_id = pa.cost_centre_id
    
    left join {{ ref('dim_vendor') }} dv 
        on cast(dv.vendor_id as varchar) = cast(pa.vendor_id as varchar)
    
    left join {{ ref('dim_product') }} dp 
        on dp.sku_id = pa.sku_id
),

-- Final fact table - only include records with variance
final_fact as (
    select
        -- Surrogate fact key
        md5(concat(sku_id, '|', vendor_id, '|', cost_centre_id, '|', order_date)) as price_variance_key,
        
        -- Dimension keys
        date_key,
        product_key,
        vendor_key,
        cost_centre_key,
        
        -- Price measures (in USD for consistency)
        avg_unit_price_usd as avg_unit_price,
        min_unit_price_usd as min_unit_price,
        max_unit_price_usd as max_unit_price,
        price_variance_usd as price_variance,
        price_stddev_usd as price_stddev,
        
        -- Volume measures
        order_line_count as order_count,
        total_quantity,
        
        -- Variance analysis measures
        price_range_ratio,
        coefficient_of_variation,
        has_significant_variance,
        
        -- Analysis period
        order_date as analysis_date,
        
        -- Audit fields
        current_timestamp as created_at
        
    from fact_with_keys
    where date_key is not null
      and product_key is not null
      and vendor_key is not null
      and cost_centre_key is not null
      and has_significant_variance = true  -- Only include records with significant variance
)

select * from final_fact
order by date_key desc, coefficient_of_variation desc, price_range_ratio desc
