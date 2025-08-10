-- Vendor performance scorecard
with vendor_performance as (
    select 
        dv.vendor_id,
        dv.vendor_name,
        dv.vendor_category,
        dv.vendor_tier,
        dv.country_code as vendor_country,
        
        -- Volume metrics
        count(distinct fol.order_id) as total_orders,
        count(*) as total_line_items,
        sum(fol.quantity) as total_quantity,
        sum(fol.line_total_aud) as total_spend_aud,
        
        -- Value metrics
        avg(fol.unit_price) as avg_unit_price,
        avg(fol.line_total_aud) as avg_line_value,
        min(fol.unit_price) as min_unit_price,
        max(fol.unit_price) as max_unit_price,
        
        -- Product diversity
        count(distinct dp.sku_id) as unique_products_supplied,
        count(distinct dp.product_category) as product_categories_count,
        
        -- Cost centre reach
        count(distinct dc.cost_centre_id) as cost_centres_served,
        
        -- Data quality (from exceptions)
        coalesce(exceptions.exception_count, 0) as data_quality_issues
        
    from dev_gold.fct_order_line fol
    join dev_gold.dim_vendor dv on fol.vendor_key = dv.vendor_key
    join dev_gold.dim_product dp on fol.product_key = dp.product_key
    join dev_gold.dim_cost_centre dc on fol.cost_centre_key = dc.cost_centre_key
    join dev_gold.dim_date dd on fol.date_key = dd.date_key
    
    -- Left join with data quality exceptions
    left join (
        select 
            vendor_key,
            count(*) as exception_count
        from dev_gold.fct_data_quality
        group by vendor_key
    ) exceptions on dv.vendor_key = exceptions.vendor_key
    
    where dd.date_actual >= current_date() - interval 12 month
    
    group by 1,2,3,4,5
),

vendor_rankings as (
    select 
        *,
        -- Performance rankings
        row_number() over (order by total_spend_aud desc) as spend_rank,
        row_number() over (order by total_orders desc) as volume_rank,
        row_number() over (order by unique_products_supplied desc) as diversity_rank,
        row_number() over (order by data_quality_issues asc, total_spend_aud desc) as quality_rank,
        
        -- Concentration metrics
        round((total_spend_aud / sum(total_spend_aud) over()) * 100, 2) as spend_concentration_percent
        
    from vendor_performance
)

select 
    vendor_name,
    vendor_category,
    vendor_tier,
    vendor_country,
    
    -- Key metrics
    total_spend_aud,
    total_orders,
    unique_products_supplied,
    cost_centres_served,
    data_quality_issues,
    
    -- Rankings and concentration
    spend_rank,
    volume_rank,
    diversity_rank,
    quality_rank,
    spend_concentration_percent,
    
    -- Value indicators
    round(avg_line_value, 2) as avg_line_value_aud,
    round(total_spend_aud / nullif(total_orders, 0), 2) as avg_order_value_aud,
    
    -- Strategic indicators
    case 
        when spend_rank <= 10 and quality_rank <= 20 then 'Strategic Partner'
        when spend_rank <= 20 and diversity_rank <= 15 then 'Key Supplier'
        when total_orders >= 50 and data_quality_issues <= 5 then 'Reliable Supplier'
        when spend_concentration_percent < 1 and total_orders < 10 then 'Consolidation Candidate'
        else 'Standard Supplier'
    end as vendor_classification
    
from vendor_rankings
order by spend_rank;
