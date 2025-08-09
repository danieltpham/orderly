{{ config(materialized='table') }}

-- Raw cost centres data from CSV
-- Bronze layer: immutable, no business logic transformations
SELECT *
FROM read_csv_auto('{{ var("source_data_path") }}/cost_centres.csv', header=true)
