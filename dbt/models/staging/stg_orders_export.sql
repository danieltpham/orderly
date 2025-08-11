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
