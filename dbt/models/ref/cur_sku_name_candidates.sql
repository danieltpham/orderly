-- cur_sku_name_candidates.sql
-- Aggregates silver.stg_orders to produce SKU-level name candidates for curation

{{ config(
    materialized='table',
    post_hook=[
        "{% if target.name == 'dev' %}
        {{ log('🔄 Exporting cur_sku_name_candidates to CSV in dev environment...', info=True) }}
        COPY (SELECT * FROM {{ this }}) TO 'data/intermediate/cur_sku_name_candidates.csv' (HEADER, DELIMITER ',')
        {% endif %}"
    ]
) }}

select
    sku_id,
    item_description_cleaned as item_description,
    count(*) as line_order_count
from {{ ref('stg_orders') }}
where sku_id is not null and item_description_cleaned is not null
group by sku_id, item_description_cleaned
order by sku_id, line_order_count desc
