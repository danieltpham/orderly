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

{% macro export_all_bronze_to_csv(output_path='data/intermediate') %}
  {% if target.name == 'dev' %}
    {{ log("🔄 Starting bronze layer CSV export...", info=True) }}
    {{ export_to_csv('raw_cost_centres', output_path, 'bronze') }}
    {{ export_to_csv('raw_exchange_rates', output_path, 'bronze') }}
    {{ export_to_csv('raw_orders', output_path, 'bronze') }}
    {{ export_to_csv('raw_vendor_master', output_path, 'bronze') }}
    {{ log("✅ Bronze layer CSV export completed!", info=True) }}
  {% else %}
    {{ log("⏭️  Skipping CSV export - not in dev environment (current: " ~ target.name ~ ")", info=True) }}
  {% endif %}
{% endmacro %}

{% macro export_all_silver_to_csv(output_path='data/intermediate') %}
  {# Add your silver models here when they exist #}
  {{ log("No silver models to export yet", info=True) }}
{% endmacro %}

{% macro export_all_gold_to_csv(output_path='data/intermediate') %}
  {# Add your gold models here when they exist #}
  {{ log("No gold models to export yet", info=True) }}
{% endmacro %}

{% macro export_all_layers_to_csv(output_path='data/intermediate') %}
  {{ export_all_bronze_to_csv(output_path) }}
  {{ export_all_silver_to_csv(output_path) }}
  {{ export_all_gold_to_csv(output_path) }}
{% endmacro %}
