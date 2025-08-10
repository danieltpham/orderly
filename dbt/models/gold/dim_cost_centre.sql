{{ config(materialized='table') }}

-- Cost centre dimension with surrogate keys
select
    -- Surrogate key (hash-based for deterministic generation)
    md5(cost_centre_id) as cost_centre_key,
    
    -- Business key and attributes
    cost_centre_id,
    cost_centre_name,
    country_code,
    
    -- Derived attributes
    case 
        when country_code = 'AU' then 'Australia'
        when country_code = 'US' then 'United States'
        else 'Unknown'
    end as country_name,
    
    case 
        when country_code = 'AU' then 'APAC'
        when country_code = 'US' then 'Americas'
        else 'Other'
    end as region,
    
    -- Metadata
    now() as created_at,
    now() as updated_at,
    true as is_active
    
from {{ ref('silver_cost_centres') }}
order by cost_centre_id
