{{ config(materialized='table') }}

-- Raw exchange rates data from CSV
-- Bronze layer: immutable, no business logic transformations
SELECT *
FROM read_csv_auto('{{ var("source_data_path") }}/exchange_rates.csv', header=true)
