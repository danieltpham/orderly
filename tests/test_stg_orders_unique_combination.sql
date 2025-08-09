-- Test to ensure unique combination of order_id, line_number, and source_filename
-- This prevents duplicate line items in our staging data

WITH duplicate_lines AS (
    SELECT 
        order_id,
        line_number,
        source_filename,
        COUNT(*) as duplicate_count
    FROM {{ ref('stg_orders') }}
    GROUP BY order_id, line_number, source_filename
    HAVING COUNT(*) > 1
)

SELECT *
FROM duplicate_lines

-- This test should return 0 rows if there are no duplicates
