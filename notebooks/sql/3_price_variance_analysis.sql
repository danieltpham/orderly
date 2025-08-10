-- Price variance analysis for procurement optimization
select 
    dp.sku_id,
    dp.sku_name,
    dp.product_category,
    dv.vendor_name,
    dc.cost_centre_name,
    dd.month_name,
    dd.year_actual,
    
    -- Price metrics
    fpv.avg_unit_price,
    fpv.min_unit_price,
    fpv.max_unit_price,
    fpv.price_variance,
    fpv.order_count,
    fpv.total_quantity,
    
    -- Variance analysis
    round(
        ((fpv.max_unit_price - fpv.min_unit_price) / nullif(fpv.avg_unit_price, 0)) * 100, 
        2
    ) as price_volatility_percent,
    
    -- Potential savings (if all orders were at min price)
    round(
        (fpv.avg_unit_price - fpv.min_unit_price) * fpv.total_quantity, 
        2
    ) as potential_savings_aud
    
from dev_gold.fct_price_variance fpv
join dev_gold.dim_product dp on fpv.product_key = dp.product_key
join dev_gold.dim_vendor dv on fpv.vendor_key = dv.vendor_key
join dev_gold.dim_cost_centre dc on fpv.cost_centre_key = dc.cost_centre_key
join dev_gold.dim_date dd on fpv.date_key = dd.date_key

where fpv.order_count >= 3  -- Only products with multiple orders
  and fpv.price_variance > 0.1  -- Significant variance
  and dd.date_actual >= current_date() - interval 12 month

order by potential_savings_aud desc, price_volatility_percent desc
limit 25;
