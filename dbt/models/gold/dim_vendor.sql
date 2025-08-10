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
        
        -- Vendor tier based on country preference
        case
            when lower(vendor_name) like '%au%' or lower(vendor_name) like '%australia%' then 'Preferred'
            else 'Standard'
        end as vendor_tier,
        
        -- Pass through fields
        country_code as country_code,
        payment_terms as payment_terms,
        tax_id as tax_id,
        city as city,
        vendor_type as vendor_type,
        preferred_currency as preferred_currency,
        credit_limit as credit_limit,
        street_address as street_address,
        postal_code as postal_code,
        contact_email as contact_email,
        contact_phone as contact_phone,
        
        -- Metadata
        now() as created_at,
        now() as updated_at,
        true as is_active
        
    from vendor_base
)

select * from vendor_enriched
order by vendor_id
