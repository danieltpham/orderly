{{ config(
    materialized='table',
    post_hook="{{ export_stg_orders_to_parquet() }}"
) }}

-- Staging layer transformation for orders data
-- Reads bronze orders and produces clean, validated staging dataset
WITH bronze_orders AS (
    SELECT *
    FROM {{ ref('raw_orders') }}
),

-- Step 1: HTML entity unescaping (REPLACE for fixed strings)
html_entities_cleaned AS (
    SELECT
        *,
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(item_description, '&amp;', '&'),
                            '&lt;', '<'
                        ),
                        '&gt;', '>'
                    ),
                    '&quot;', '"'
                ),
                '&apos;', ''''
            ),
            '&nbsp;', ' '
        ) AS desc_step1
    FROM bronze_orders
),

-- Step 2: Remove HTML tags (REGEXP_REPLACE, double pass for robustness)
html_tags_removed AS (
    SELECT
        *,
        REGEXP_REPLACE(
            REGEXP_REPLACE(desc_step1, '<[^>]+>', ''),
            '</[^>]+>', ''
        ) AS desc_step2
    FROM html_entities_cleaned
),

-- Step 3: Unicode dash cleanup (REPLACE for fixed strings)
unicode_dashes_cleaned AS (
    SELECT
        *,
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(desc_step2, '&#x2013;', '-'),
                    '&#x2014;', '-'
                ),
                '&#8211;', '-'
            ),
            '&#8212;', '-'
        ) AS desc_step3
    FROM html_tags_removed
),

-- Step 4: UTF-8 encoding artifacts cleanup (REPLACE for fixed strings)
utf8_artifacts_cleaned AS (
    SELECT
        *,
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(desc_step3, 'Ã¢â‚¬â€œ', '-'),
                                'Ã¢â‚¬â€', '-'
                            ),
                            'â€“', '-'
                        ),
                        'â€”', '-'
                    ),
                    '–', '-'
                ),
                '—', '-'
            ),
            'Â', ''
        ) AS desc_step4
    FROM unicode_dashes_cleaned
),

-- Step 5: Whitespace normalization (REGEXP_REPLACE)
final_text_cleaned AS (
    SELECT
        *,
        LOWER(TRIM(REGEXP_REPLACE(desc_step4, '\s+', ' '))) AS item_description_cleaned
    FROM utf8_artifacts_cleaned
),

cleaned_orders AS (
    SELECT
        -- Column selection and renaming to canonical snake_case
        order_id,
        line_number,
        sku_id,
        item_description AS item_description_original,
        item_description_cleaned,
        
        -- Safe numeric coercion
        TRY_CAST(quantity AS DOUBLE) AS quantity,
        TRY_CAST(unit_price AS DOUBLE) AS unit_price,
        
        currency,
        brand AS vendor_brand,  -- Use brand instead of vendor_id (actual column name)
        cost_centre_id,
        country_code,
        requisitioner,
        approval_status,
        delivery_date,
        
        -- Safe date parsing
        TRY_CAST(order_date AS DATE) AS order_date,
        TRY_CAST(order_timestamp AS TIMESTAMP) AS created_timestamp,
        
        -- Source filename
        filename AS source_filename,
        
        -- Non-product hint detection
        CASE 
            WHEN LOWER(item_description) LIKE '%gst%' 
                OR LOWER(item_description) LIKE '%tax%'
                OR LOWER(item_description) LIKE '%shipping%'
                OR LOWER(item_description) LIKE '%freight%'
                OR LOWER(item_description) LIKE '%delivery%'
                OR LOWER(item_description) LIKE '%handling%'
                OR LOWER(item_description) LIKE '%fee%'
                OR LOWER(item_description) LIKE '%charge%'
                OR LOWER(item_description) LIKE '%setup%'
                OR LOWER(item_description) LIKE '%installation%'
                OR LOWER(item_description) LIKE '%service%'
                OR LOWER(item_description) LIKE '%support%'
                OR LOWER(item_description) LIKE '%warranty%'
                OR LOWER(item_description) LIKE '%insurance%'
                OR LOWER(item_description) LIKE '%discount%'
                OR LOWER(item_description) LIKE '%adjustment%'
                OR LOWER(item_description) LIKE '%credit%'
            THEN TRUE
            ELSE FALSE
        END AS non_product_hint,
        
        -- Deterministic line UID (using SHA256 hash)
        hash(
            CONCAT(
                COALESCE(order_id, ''),
                '|',
                COALESCE(CAST(line_number AS VARCHAR), ''),
                '|', 
                COALESCE(sku_id, ''),
                '|',
                COALESCE(filename, '')
            )
        ) AS line_uid
        
    FROM final_text_cleaned
)

SELECT 
    order_id,
    line_number,
    sku_id,
    item_description_original,
    item_description_cleaned,
    quantity,
    unit_price,
    currency,
    vendor_brand,
    cost_centre_id,
    country_code,
    requisitioner,
    approval_status,
    delivery_date,
    order_date,
    source_filename,
    created_timestamp,
    non_product_hint,
    line_uid
FROM cleaned_orders
