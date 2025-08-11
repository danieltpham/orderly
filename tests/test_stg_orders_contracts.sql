-- Test to validate staging orders data contracts
-- This test ensures the staging transformation meets business requirements

SELECT 
    'Data Contract Validation' AS test_name,
    COUNT(*) AS total_rows,
    COUNT(CASE WHEN order_id IS NULL THEN 1 END) AS null_order_ids,
    COUNT(CASE WHEN sku_id IS NULL THEN 1 END) AS null_sku_ids,
    COUNT(CASE WHEN order_date IS NULL THEN 1 END) AS null_order_dates,
    COUNT(CASE WHEN line_number < 1 THEN 1 END) AS invalid_line_numbers,
    COUNT(CASE WHEN quantity < 0 THEN 1 END) AS negative_quantities,
    COUNT(CASE WHEN unit_price < 0 THEN 1 END) AS negative_prices,
    COUNT(DISTINCT line_uid) AS unique_line_uids,
    COUNT(CASE WHEN non_product_hint NOT IN (TRUE, FALSE) THEN 1 END) AS invalid_non_product_hints

FROM {{ ref('stg_orders') }}

-- This test should return one row with all validation counts
-- If any of the "problem" counts are > 0, the test should be reviewed
