-- Export silver layer data to CSV files (only in dev environment)
-- This model runs after all silver models are complete

{% if target.name == 'dev' %}
  {{ config(
      materialized='table',
      post_hook="{{ export_all_silver_to_csv('data/intermediate') }}"
  ) }}
{% else %}
  {{ config(materialized='table') }}
{% endif %}

SELECT 
  'Silver export completed' as status, 
  current_timestamp as exported_at,
  '{{ target.name }}' as environment,
  {% if target.name == 'dev' %}
    'CSV files exported to data/intermediate' as export_status
  {% else %}
    'CSV export skipped (not dev environment)' as export_status
  {% endif %}
