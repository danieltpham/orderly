-- Executive KPI dashboard
with current_period as (
    select 
        -- Time periods
        'Current Month' as period_name,
        sum(fol.line_total_aud) as total_spend_aud,
        count(distinct fol.order_id) as total_orders,
        count(*) as total_line_items,
        count(distinct dv.vendor_id) as active_vendors,
        count(distinct dp.sku_id) as unique_products,
        avg(fol.line_total_aud) as avg_line_value
        
    from dev_gold.fct_order_line fol
    join dev_gold.dim_date dd on fol.date_key = dd.date_key
    join dev_gold.dim_vendor dv on fol.vendor_key = dv.vendor_key
    join dev_gold.dim_product dp on fol.product_key = dp.product_key
    
    where dd.year_actual = extract(year from current_date())
      and dd.month_number = extract(month from current_date())
      
    union all
    
    select 
        'Previous Month' as period_name,
        sum(fol.line_total_aud) as total_spend_aud,
        count(distinct fol.order_id) as total_orders,
        count(*) as total_line_items,
        count(distinct dv.vendor_id) as active_vendors,
        count(distinct dp.sku_id) as unique_products,
        avg(fol.line_total_aud) as avg_line_value
        
    from dev_gold.fct_order_line fol
    join dev_gold.dim_date dd on fol.date_key = dd.date_key
    join dev_gold.dim_vendor dv on fol.vendor_key = dv.vendor_key
    join dev_gold.dim_product dp on fol.product_key = dp.product_key
    
    where dd.date_actual >= date_trunc('month', current_date()) - interval 1 month
      and dd.date_actual < date_trunc('month', current_date())
      
    union all
    
    select 
        'Year to Date' as period_name,
        sum(fol.line_total_aud) as total_spend_aud,
        count(distinct fol.order_id) as total_orders,
        count(*) as total_line_items,
        count(distinct dv.vendor_id) as active_vendors,
        count(distinct dp.sku_id) as unique_products,
        avg(fol.line_total_aud) as avg_line_value
        
    from dev_gold.fct_order_line fol
    join dev_gold.dim_date dd on fol.date_key = dd.date_key
    join dev_gold.dim_vendor dv on fol.vendor_key = dv.vendor_key
    join dev_gold.dim_product dp on fol.product_key = dp.product_key
    
    where dd.year_actual = extract(year from current_date())
),

quality_metrics as (
    select 
        count(*) as total_exceptions,
        avg(fuzz_score) as avg_data_quality_score
    from dev_gold.fct_data_quality fdq
    join dev_gold.dim_date dd on fdq.date_key = dd.date_key
    where dd.year_actual = extract(year from current_date())
      and dd.month_number = extract(month from current_date())
)

select 
    cp.*,
    qm.total_exceptions,
    round(qm.avg_data_quality_score, 2) as avg_data_quality_score,
    
    -- Calculate month-over-month growth
    case 
        when cp.period_name = 'Current Month' then
            round(
                (cp.total_spend_aud - lag(cp.total_spend_aud) over (order by cp.period_name desc)) / 
                nullif(lag(cp.total_spend_aud) over (order by cp.period_name desc), 0) * 100, 
                2
            )
        else null
    end as mom_spend_growth_percent
    
from current_period cp
cross join quality_metrics qm
order by 
    case cp.period_name 
        when 'Current Month' then 1
        when 'Previous Month' then 2
        when 'Year to Date' then 3
    end;
