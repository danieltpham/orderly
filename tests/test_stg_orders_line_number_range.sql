-- Test to ensure line_number values are valid (>= 1)
-- Line numbers should always be positive integers

SELECT 
    order_id,
    line_number,
    source_filename
FROM {{ ref('stg_orders') }}
WHERE line_number IS NULL 
   OR line_number < 1

-- This test should return 0 rows if all line numbers are valid
