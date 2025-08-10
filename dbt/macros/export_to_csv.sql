{% macro export_to_csv(model_name, output_path, layer='bronze') %}
  {% set export_query %}
    COPY (
      SELECT * FROM {{ ref(model_name) }}
    ) TO '{{ output_path }}/{{ layer }}_{{ model_name }}.csv' 
    (HEADER, DELIMITER ',')
  {% endset %}
  
  {{ log("Exporting " ~ model_name ~ " to " ~ output_path ~ "/" ~ layer ~ "_" ~ model_name ~ ".csv", info=True) }}
  
  {% if execute %}
    {% do run_query(export_query) %}
    {{ log("✅ Successfully exported " ~ model_name, info=True) }}
  {% endif %}
{% endmacro %}

{% macro export_to_csv_posthook(output_path, layer='bronze') %}
  {% set model_name = this.name %}
  {% set export_query %}
    COPY (
      SELECT * FROM {{ this }}
    ) TO '{{ output_path }}/{{ layer }}_{{ model_name }}.csv' 
    (HEADER, DELIMITER ',')
  {% endset %}
  
  {{ log("Exporting " ~ model_name ~ " to " ~ output_path ~ "/" ~ layer ~ "_" ~ model_name ~ ".csv", info=True) }}
  
  {% if execute %}
    {% do run_query(export_query) %}
    {{ log("✅ Successfully exported " ~ model_name, info=True) }}
  {% endif %}
{% endmacro %}

{% macro export_all_bronze_to_csv(output_path='data/intermediate') %}
  {% if target.name == 'dev' %}
    {{ log("🔄 Starting bronze layer CSV export...", info=True) }}
    {{ export_to_csv('raw_cost_centres', output_path, 'bronze') }}
    {{ export_to_csv('raw_exchange_rates', output_path, 'bronze') }}
    {{ export_to_csv('raw_orders', output_path, 'bronze') }}
    {{ export_to_csv('raw_vendor_master', output_path, 'bronze') }}
    {{ log("✅ Bronze layer CSV export completed!", info=True) }}
  {% else %}
    {{ log("⏭️  Skipping bronze CSV export - not in dev environment (current: " ~ target.name ~ ")", info=True) }}
  {% endif %}
{% endmacro %}

{% macro export_all_silver_to_csv(output_path='data/intermediate') %}
  {% if target.name == 'dev' %}
    {{ log("🔄 Starting silver layer CSV export...", info=True) }}
    {{ export_to_csv('silver_cost_centres', output_path, 'silver') }}
    {{ export_to_csv('silver_exchange_rates', output_path, 'silver') }}
    {{ export_to_csv('silver_vendor_master', output_path, 'silver') }}
    {{ export_to_csv('silver_orders_valid', output_path, 'silver') }}
    {{ export_to_csv('silver_orders_exceptions', output_path, 'silver') }}
    {{ export_to_csv('silver_orders_nonproduct', output_path, 'silver') }}
    {{ log("✅ Silver layer CSV export completed!", info=True) }}
  {% else %}
    {{ log("⏭️  Skipping silver CSV export - not in dev environment (current: " ~ target.name ~ ")", info=True) }}
  {% endif %}
{% endmacro %}

{% macro export_all_gold_to_csv(output_path='data/exports') %}
  {{ log("🔄 Starting gold layer CSV export...", info=True) }}
  {{ export_to_csv('dim_cost_centre', output_path, 'gold') }}
  {{ export_to_csv('dim_date', output_path, 'gold') }}
  {{ export_to_csv('dim_product', output_path, 'gold') }}
  {{ export_to_csv('dim_vendor', output_path, 'gold') }}
  {{ export_to_csv('fct_data_quality', output_path, 'gold') }}
  {{ export_to_csv('fct_order_line', output_path, 'gold') }}
  {{ export_to_csv('fct_price_variance', output_path, 'gold') }}
  {{ log("✅ Gold layer CSV export completed!", info=True) }}
{% endmacro %}

{% macro export_all_layers_to_csv() %}
  {{ export_all_bronze_to_csv('data/intermediate') }}
  {{ export_all_silver_to_csv('data/intermediate') }}
  {{ export_all_gold_to_csv('data/exports') }}
{% endmacro %}
