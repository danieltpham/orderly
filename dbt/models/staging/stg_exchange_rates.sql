{{ config(materialized='table') }}

-- Staging exchange rates - Daily USD to AUD rates with interpolation for missing dates
with raw_rates as (
    select
        date,
        from_currency,
        to_currency,
        exchange_rate
    from {{ ref('raw_exchange_rates') }}
),

-- Get USD->AUD rates directly from source
usd_to_aud_direct as (
    select
        date,
        exchange_rate,
        'SOURCE' as rate_source
    from raw_rates
    where from_currency = 'USD' and to_currency = 'AUD'
),

-- Calculate USD->AUD from AUD->USD (inverse)
usd_to_aud_calculated as (
    select
        date,
        1.0 / exchange_rate as exchange_rate,
        'CALCULATED' as rate_source
    from raw_rates
    where from_currency = 'AUD' and to_currency = 'USD'
        and exchange_rate > 0  -- Avoid division by zero
),

-- Combine both sources, prioritizing direct rates
combined_rates as (
    select * from usd_to_aud_direct
    union all
    select * from usd_to_aud_calculated
),

-- Remove duplicates, prioritizing SOURCE over CALCULATED
deduplicated_rates as (
    select
        date,
        exchange_rate,
        rate_source,
        row_number() over (
            partition by date 
            order by case when rate_source = 'SOURCE' then 1 else 2 end
        ) as rn
    from combined_rates
),

-- Get unique rates per date
base_rates as (
    select
        date,
        exchange_rate,
        rate_source
    from deduplicated_rates
    where rn = 1
),

-- Create complete date range using DuckDB's generate_series
date_range as (
    select d::date as date
    from generate_series(
        (select min(date) from base_rates),
        (select max(date) from base_rates),
        interval '1 day'
    ) as t(d)
),

-- Join dates with known rates
dates_with_rates as (
    select 
        dr.date,
        br.exchange_rate,
        br.rate_source
    from date_range dr
    left join base_rates br on dr.date = br.date
),

-- Add row numbers and forward/backward fill for interpolation
rates_with_context as (
    select 
        date,
        exchange_rate,
        rate_source,
        -- Get the last known rate before this date
        last_value(exchange_rate ignore nulls) over (
            order by date 
            rows between unbounded preceding and current row
        ) as prev_rate,
        -- Get the last known date before this date  
        last_value(case when exchange_rate is not null then date end ignore nulls) over (
            order by date 
            rows between unbounded preceding and current row
        ) as prev_date,
        -- Get the next known rate after this date
        first_value(exchange_rate ignore nulls) over (
            order by date 
            rows between current row and unbounded following
        ) as next_rate,
        -- Get the next known date after this date
        first_value(case when exchange_rate is not null then date end ignore nulls) over (
            order by date 
            rows between current row and unbounded following
        ) as next_date
    from dates_with_rates
),

-- Calculate interpolated rates
final_rates as (
    select
        date,
        case 
            -- Use actual rate if available
            when exchange_rate is not null then exchange_rate
            -- Linear interpolation between prev and next
            when prev_rate is not null and next_rate is not null and prev_date != next_date then
                prev_rate + ((next_rate - prev_rate) * 
                    ((date - prev_date) / (next_date - prev_date)))
            -- Forward fill if no next rate
            when prev_rate is not null then prev_rate
            -- Backward fill if no prev rate
            when next_rate is not null then next_rate
            else null
        end as exchange_rate,
        case 
            when exchange_rate is not null then rate_source
            when prev_rate is not null and next_rate is not null and prev_date != next_date then 'INTERPOLATED'
            when prev_rate is not null or next_rate is not null then 'FORWARD_FILLED'
            else null
        end as rate_source
    from rates_with_context
)

select
    date,
    'USD' as from_currency,
    'AUD' as to_currency,
    exchange_rate,
    rate_source
from final_rates
where exchange_rate is not null
order by date
