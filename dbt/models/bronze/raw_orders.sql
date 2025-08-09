{{ config(materialized='table') }}

-- Raw orders data from JSON files
-- Bronze layer: immutable, no business logic transformations
SELECT *
FROM read_json_auto('{{ var("source_data_path") }}/orders_*.json')