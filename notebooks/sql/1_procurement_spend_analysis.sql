-- Top procurement spend by cost centre and vendor
with quarterly_spend as (
    select 
        dc.cost_centre_name,
        dc.cost_centre_name,
        dv.vendor_name,
        dv.vendor_category,
        dp.product_category,
        dd.quarter,
        dd.year_actual,
        
        -- Spend metrics
        sum(fol.line_total_aud) as total_spend_aud,
        sum(fol.quantity) as total_quantity,
        count(*) as order_line_count,
        avg(fol.unit_price) as avg_unit_price
        
    from dev_gold.fct_order_line fol
    join dev_gold.dim_date dd on fol.date_key = dd.date_key
    join dev_gold.dim_cost_centre dc on fol.cost_centre_key = dc.cost_centre_key
    join dev_gold.dim_vendor dv on fol.vendor_key = dv.vendor_key
    join dev_gold.dim_product dp on fol.product_key = dp.product_key
    
    where dd.date_actual >= current_date() - interval 3 month
    
    group by 1,2,3,4,5,6,7
)

select 
    cost_centre_name,
    vendor_name,
    product_category,
    total_spend_aud,
    total_quantity,
    order_line_count,
    round(total_spend_aud / nullif(total_quantity, 0), 2) as avg_cost_per_unit
    
from quarterly_spend
order by total_spend_aud desc
limit 20;
