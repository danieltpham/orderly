{{ config(materialized='table') }}

-- Product dimension combining seed data with discovered SKUs from orders
with sku_from_seed as (
    select
        sku_id,
        canonical_name,
        source as source_type,
        true as from_seed
    from {{ ref('ref_sku_names') }}
),

sku_from_orders as (
    select distinct
        sku_id,
        item_description_cleaned as canonical_name,
        'auto_discovered' as source_type,
        false as from_seed
    from {{ ref('silver_orders_valid') }}
    where sku_id is not null
    and sku_id not in (select sku_id from sku_from_seed)
),

all_skus as (
    select * from sku_from_seed
    union all
    select * from sku_from_orders
),

product_with_categories as (
    select
        -- Surrogate key
        md5(sku_id) as product_key,
        
        -- Business key and core attributes
        sku_id,
        canonical_name,
        source_type,
        
        -- Product categorization based on name patterns
        case
            when lower(canonical_name) like '%keyboard%' or lower(canonical_name) like '%kb%' then 'Keyboards'
            when lower(canonical_name) like '%monitor%' or lower(canonical_name) like '%display%' or lower(canonical_name) like '%screen%' then 'Monitors'
            when lower(canonical_name) like '%webcam%' or lower(canonical_name) like '%camera%' or lower(canonical_name) like '%cam%' then 'Webcams'
            when lower(canonical_name) like '%headset%' or lower(canonical_name) like '%headphone%' or lower(canonical_name) like '%earphone%' or lower(canonical_name) like '%earbuds%' then 'Headsets'
            when lower(canonical_name) like '%cable%' or lower(canonical_name) like '%cord%' or lower(canonical_name) like '%wire%' then 'Cables'
            when lower(canonical_name) like '%adapter%' or lower(canonical_name) like '%adaptor%' or lower(canonical_name) like '%converter%' then 'Adapters'
            when lower(canonical_name) like '%drive%' or lower(canonical_name) like '%storage%' or lower(canonical_name) like '%tb%' or lower(canonical_name) like '%gb%' then 'Storage'
            when lower(canonical_name) like '%mouse%' or lower(canonical_name) like '%trackpad%' then 'Input Devices'
            when lower(canonical_name) like '%speaker%' or lower(canonical_name) like '%audio%' then 'Audio'
            when lower(canonical_name) like '%usb%' or lower(canonical_name) like '%connector%' then 'Connectivity'
            else 'Other'
        end as product_category,
        
        -- Sub-category for more granular analysis
        case
            when lower(canonical_name) like '%wireless%' then 'Wireless'
            when lower(canonical_name) like '%wired%' then 'Wired'
            when lower(canonical_name) like '%usb-c%' or lower(canonical_name) like '%usb c%' then 'USB-C'
            when lower(canonical_name) like '%hdmi%' then 'HDMI'
            when lower(canonical_name) like '%4k%' or lower(canonical_name) like '%uhd%' then '4K/UHD'
            when lower(canonical_name) like '%compact%' or lower(canonical_name) like '%mini%' then 'Compact'
            when lower(canonical_name) like '%external%' then 'External'
            when lower(canonical_name) like '%mechanical%' or lower(canonical_name) like '%mech%' then 'Mechanical'
            else 'Standard'
        end as product_subcategory,
        
        -- Brand extraction (simplified - looking for known patterns)
        case
            when lower(canonical_name) like '%connectpro%' then 'ConnectPro'
            when lower(canonical_name) like '%viewmaster%' then 'ViewMaster'
            when lower(canonical_name) like '%datavault%' then 'DataVault'
            when lower(canonical_name) like '%techcorp%' then 'TechCorp'
            when lower(canonical_name) like '%officesupply%' then 'OfficeSupply Co'
            else 'Generic'
        end as product_brand,
        
        -- Source tracking
        from_seed,
        case 
            when source_type = 'approved_seed' then true
            else false
        end as is_approved,
        
        -- Metadata
        now() as created_at,
        now() as updated_at,
        true as is_active
        
    from all_skus
)

select * from product_with_categories
order by 
    case when from_seed then 0 else 1 end,  -- Seed records first
    sku_id
