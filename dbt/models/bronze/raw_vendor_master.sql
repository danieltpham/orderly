{{ config(materialized='table') }}

-- Raw vendor master data from CSV
-- Bronze layer: immutable, no business logic transformations
SELECT *
FROM read_csv_auto('{{ var("source_data_path") }}/vendor_master.csv', header=true)