-- Seasonal procurement trend analysis
with monthly_trends as (
    select 
        dd.month,
        dd.quarter,
        dp.product_category,
        dp.product_brand,
        
        -- Monthly aggregations
        sum(fol.line_total_aud) as monthly_spend_aud,
        sum(fol.quantity) as monthly_quantity,
        count(distinct fol.order_id) as monthly_orders,
        count(*) as monthly_line_items,
        avg(fol.unit_price) as avg_unit_price
        
    from dev_gold.fct_order_line fol
    join dev_gold.dim_date dd on fol.date_key = dd.date_key
    join dev_gold.dim_product dp on fol.product_key = dp.product_key
    
    where dd.year >= extract(year from current_date()) - 2  -- Last 2 years
    
    group by 1,2,3,4
),

category_averages as (
    select 
        product_category,
        avg(monthly_spend_aud) as avg_monthly_spend,
        stddev(monthly_spend_aud) as stddev_monthly_spend
    from monthly_trends
    group by 1
)

select 
    mt.month,
    mt.quarter,
    mt.product_category,
    mt.product_brand,
    mt.monthly_spend_aud,
    mt.monthly_quantity,
    mt.monthly_orders,
    
    -- Seasonality indicators
    round(
        (mt.monthly_spend_aud - ca.avg_monthly_spend) / nullif(ca.stddev_monthly_spend, 0), 
        2
    ) as spend_seasonality_zscore,
    
    -- Trend indicators
    round(
        (mt.monthly_spend_aud / nullif(ca.avg_monthly_spend, 0) - 1) * 100, 
        2
    ) as spend_variance_from_avg_percent
    
from monthly_trends mt
join category_averages ca on mt.product_category = ca.product_category

order by mt.product_category, mt.month;
