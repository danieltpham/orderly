{{ config(materialized='table') }}

-- Date dimension with comprehensive date attributes
-- Covers a reasonable range around our data dates
with date_spine as (
    select 
        date_column as date_actual
    from generate_series(
        '2025-06-01'::date, 
        '2025-08-31'::date, 
        interval '1 day'
    ) as t(date_column)
),

exchange_rates as (
    select 
        date,
        exchange_rate as usd_to_aud
    from {{ ref('silver_exchange_rates') }}
    where from_currency = 'USD' and to_currency = 'AUD'
),

date_with_attributes as (
    select
        date_actual,
        
        -- Date key as integer YYYYMMDD
        extract(year from date_actual) * 10000 + extract(month from date_actual) * 100 + extract(day from date_actual) as date_key,
        
        -- Basic date parts
        extract(year from date_actual) as year,
        extract(quarter from date_actual) as quarter,
        extract(month from date_actual) as month,
        extract(week from date_actual) as week,
        extract(day from date_actual) as day_of_month,
        extract(dow from date_actual) as day_of_week,
        dayname(date_actual) as day_name,
        monthname(date_actual) as month_name,
        
        -- Derived attributes
        case when extract(dow from date_actual) in (0, 6) then true else false end as is_weekend,
        
        -- Fiscal year (assuming July 1 - June 30, common in AU/US)
        case 
            when extract(month from date_actual) >= 7 then extract(year from date_actual) + 1
            else extract(year from date_actual)
        end as fiscal_year,
        
        case 
            when extract(month from date_actual) in (7, 8, 9) then 1
            when extract(month from date_actual) in (10, 11, 12) then 2  
            when extract(month from date_actual) in (1, 2, 3) then 3
            when extract(month from date_actual) in (4, 5, 6) then 4
        end as fiscal_quarter,
        
        -- Holiday flags (basic set - can be enhanced)
        case 
            when extract(month from date_actual) = 1 and extract(day from date_actual) = 1 then true  -- New Year
            when extract(month from date_actual) = 12 and extract(day from date_actual) = 25 then true -- Christmas
            when extract(month from date_actual) = 1 and extract(day from date_actual) = 26 then true -- Australia Day
            when extract(month from date_actual) = 4 and extract(day from date_actual) = 25 then true -- ANZAC Day
            else false
        end as is_holiday
        
    from date_spine
)

select
    d.*,
    coalesce(er.usd_to_aud, 1.5) as usd_to_aud  -- Default fallback rate
from date_with_attributes d
left join exchange_rates er on d.date_actual = er.date
order by date_actual
