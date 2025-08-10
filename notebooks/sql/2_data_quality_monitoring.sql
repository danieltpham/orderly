-- Data quality exception analysis
with exception_summary as (
    select 
        dc.cost_centre_name,
        dc.cost_centre_code,
        fdq.exception_type,
        dd.month_name,
        dd.year_actual,
        
        -- Quality metrics
        count(*) as exception_count,
        avg(fdq.fuzz_score) as avg_fuzz_score,
        avg(fdq.vendor_fuzz_score) as avg_vendor_fuzz_score,
        
        -- Flags breakdown
        sum(case when fdq.flag_missing_in_seed then 1 else 0 end) as missing_in_seed_count,
        sum(case when fdq.flag_name_mismatch then 1 else 0 end) as name_mismatch_count,
        sum(case when fdq.flag_vendor_mismatch then 1 else 0 end) as vendor_mismatch_count
        
    from dev_gold.fct_data_quality fdq
    join dev_gold.dim_date dd on fdq.date_key = dd.date_key
    join dev_gold.dim_cost_centre dc on fdq.cost_centre_key = dc.cost_centre_key
    
    where dd.date_actual >= current_date() - interval 6 month
    
    group by 1,2,3,4,5
),

total_orders as (
    select 
        dc.cost_centre_name,
        count(*) as total_order_lines
    from dev_gold.fct_order_line fol
    join dev_gold.dim_cost_centre dc on fol.cost_centre_key = dc.cost_centre_key
    join dev_gold.dim_date dd on fol.date_key = dd.date_key
    where dd.date_actual >= current_date() - interval 6 month
    group by 1
)

select 
    es.cost_centre_name,
    es.exception_type,
    es.exception_count,
    round(es.avg_fuzz_score, 2) as avg_fuzz_score,
    es.missing_in_seed_count,
    es.name_mismatch_count,
    es.vendor_mismatch_count,
    
    -- Calculate exception rate
    round(
        (es.exception_count * 100.0) / nullif(tot.total_order_lines, 0), 
        2
    ) as exception_rate_percent
    
from exception_summary es
left join total_orders tot on es.cost_centre_name = tot.cost_centre_name
order by es.exception_count desc;
