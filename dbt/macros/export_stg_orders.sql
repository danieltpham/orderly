{% macro export_stg_orders_to_parquet() %}
  
  {% set query %}
    COPY (
      SELECT * FROM {{ this }}
    ) TO '{{ var("project_root", ".") }}/data/intermediate/stg_orders.csv' 
    (FORMAT CSV, HEADER)
  {% endset %}

  {% if execute %}
    {% do run_query(query) %}
    {{ log("✅ Exported stg_orders to CSV: data/intermediate/stg_orders.csv", info=True) }}
  {% endif %}

{% endmacro %}
