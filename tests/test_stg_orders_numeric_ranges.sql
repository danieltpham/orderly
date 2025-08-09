-- Test to ensure quantity and unit_price values are non-negative
-- These should be >= 0 for business logic compliance

SELECT 
    order_id,
    line_number,
    sku_id,
    quantity,
    unit_price,
    source_filename
FROM {{ ref('stg_orders') }}
WHERE (quantity IS NOT NULL AND quantity < 0)
   OR (unit_price IS NOT NULL AND unit_price < 0)

-- This test should return 0 rows if all quantities and prices are valid
