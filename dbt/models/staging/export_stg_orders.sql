-- Export staging orders data to CSV file (only in dev environment)
-- This model runs after stg_orders is complete

{% if target.name == 'dev' %}
  {{ config(
      materialized='table',
      post_hook="{{ export_stg_orders() }}"
  ) }}
{% else %}
  {{ config(materialized='table') }}
{% endif %}

SELECT 
  'Staging orders export completed' as status, 
  current_timestamp as exported_at,
  '{{ target.name }}' as environment,
  {% if target.name == 'dev' %}
    'CSV file exported to data/intermediate/stg_orders.csv' as export_status
  {% else %}
    'CSV export skipped (not dev environment)' as export_status
  {% endif %}

{% macro export_stg_orders() %}
  
  {% set query %}
    COPY (
      SELECT * FROM {{ ref('stg_orders') }}
    ) TO '{{ var("project_root", ".") }}/data/intermediate/stg_orders.csv' 
    (FORMAT CSV, HEADER)
  {% endset %}

  {% if execute %}
    {% do run_query(query) %}
    {{ log("✅ Exported stg_orders to CSV: data/intermediate/stg_orders.csv", info=True) }}
  {% endif %}

{% endmacro %}
