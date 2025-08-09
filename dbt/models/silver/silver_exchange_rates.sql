{{ config(materialized='table') }}

-- Silver exchange rates - Daily USD to AUD rates with interpolation for missing dates
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

-- Create date spine for interpolation
date_range as (
    select
        date_add('day', seq, (select min(date) from base_rates)) as date
    from (
        select row_number() over () - 1 as seq
        from (select 1 as x) t1
        cross join (select 1 as x) t2
        cross join (select 1 as x) t3
        cross join (select 1 as x) t4
        cross join (select 1 as x) t5
        cross join (select 1 as x) t6
        cross join (select 1 as x) t7
        cross join (select 1 as x) t8
        cross join (select 1 as x) t9
        cross join (select 1 as x) t10
        cross join (select 1 as x) t11
        cross join (select 1 as x) t12
    ) seq_table
    where date_add('day', seq, (select min(date) from base_rates)) <= (select max(date) from base_rates)
),

-- Join date spine with rates and fill gaps with interpolation
rates_with_interpolation as (
    select
        dr.date,
        coalesce(
            br.exchange_rate,
            -- Linear interpolation between previous and next known rates
            (
                select 
                    prev.exchange_rate + 
                    ((next.exchange_rate - prev.exchange_rate) * 
                     (date_diff('day', prev.date, dr.date) * 1.0 / 
                      date_diff('day', prev.date, next.date)))
                from (
                    select date, exchange_rate
                    from base_rates
                    where date < dr.date
                    order by date desc
                    limit 1
                ) prev
                cross join (
                    select date, exchange_rate
                    from base_rates
                    where date > dr.date
                    order by date asc
                    limit 1
                ) next
                where prev.date is not null and next.date is not null
            )
        ) as exchange_rate,
        coalesce(br.rate_source, 'INTERPOLATED') as rate_source
    from date_range dr
    left join base_rates br on dr.date = br.date
)

select
    date,
    'USD' as from_currency,
    'AUD' as to_currency,
    exchange_rate,
    rate_source
from rates_with_interpolation
where exchange_rate is not null
order by date
