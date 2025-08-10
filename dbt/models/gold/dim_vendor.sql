{{ config(materialized='table') }}

-- Vendor dimension with enrichment and categorization
with vendor_base as (
    select * from {{ ref('silver_vendor_master') }}
),

vendor_enriched as (
    select
        -- Surrogate key
        md5(vendor_id) as vendor_key,
        
        -- Business key and core attributes
        vendor_id,
        vendor_name,
        
        -- Derive vendor category from name patterns
        case
            when lower(vendor_name) like '%tech%' or lower(vendor_name) like '%solutions%' then 'Technology'
            when lower(vendor_name) like '%office%' or lower(vendor_name) like '%supply%' then 'Office Supplies'
            when lower(vendor_name) like '%electronics%' or lower(vendor_name) like '%computer%' then 'Electronics'
            when lower(vendor_name) like '%connect%' then 'Connectivity'
            when lower(vendor_name) like '%view%' or lower(vendor_name) like '%display%' then 'Display Equipment'
            when lower(vendor_name) like '%data%' or lower(vendor_name) like '%storage%' then 'Data & Storage'
            else 'General'
        end as vendor_category,
        
        -- Derive country preference (placeholder - can be enhanced with actual data)
        case
            when lower(vendor_name) like '%au%' or lower(vendor_name) like '%australia%' then 'AU'
            when lower(vendor_name) like '%us%' or lower(vendor_name) like '%america%' then 'US'
            else 'Unknown'
        end as country_code,
        
        -- Vendor tier based on country preference
        case
            when lower(vendor_name) like '%au%' or lower(vendor_name) like '%australia%' then 'Preferred'
            else 'Standard'
        end as vendor_tier,
        
        -- Placeholder fields for future enrichment
        null as payment_terms,
        null as tax_id,
        null as city,
        null as vendor_type,
        null as preferred_currency,
        null as credit_limit,
        null as street_address,
        null as postal_code,
        null as contact_email,
        null as contact_phone,
        
        -- Metadata
        now() as created_at,
        now() as updated_at,
        true as is_active
        
    from vendor_base
)

select * from vendor_enriched
order by vendor_id
