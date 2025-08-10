-- Regional procurement comparison
with regional_metrics as (
    select 
        dc.country_code,
        dc.region,
        dc.cost_centre_name,
        dd.quarter_name,
        dd.year_actual,
        dv.vendor_category,
        
        -- Procurement metrics
        count(distinct fol.order_id) as unique_orders,
        count(*) as total_line_items,
        sum(fol.line_total_aud) as total_spend_aud,
        sum(fol.line_total_usd) as total_spend_usd,
        avg(fol.line_total_aud) as avg_line_value_aud,
        
        -- Vendor diversity
        count(distinct dv.vendor_id) as unique_vendors,
        
        -- Product diversity  
        count(distinct dp.sku_id) as unique_products
        
    from dev_gold.fct_order_line fol
    join dev_gold.dim_date dd on fol.date_key = dd.date_key
    join dev_gold.dim_cost_centre dc on fol.cost_centre_key = dc.cost_centre_key
    join dev_gold.dim_vendor dv on fol.vendor_key = dv.vendor_key
    join dev_gold.dim_product dp on fol.product_key = dp.product_key
    
    where dd.date_actual >= current_date() - interval 12 month
    
    group by 1,2,3,4,5,6
)

select 
    country_code,
    region,
    vendor_category,
    
    -- Aggregated metrics by region
    sum(total_spend_aud) as region_total_spend_aud,
    avg(avg_line_value_aud) as avg_line_value_aud,
    sum(unique_orders) as total_orders,
    sum(total_line_items) as total_line_items,
    
    -- Efficiency metrics
    round(sum(total_spend_aud) / nullif(sum(unique_orders), 0), 2) as avg_order_value,
    round(sum(total_line_items) / nullif(sum(unique_orders), 0), 2) as avg_lines_per_order,
    
    -- Diversity metrics
    avg(unique_vendors) as avg_vendors_per_cc,
    avg(unique_products) as avg_products_per_cc
    
from regional_metrics
group by 1,2,3
order by country_code, region_total_spend_aud desc;
