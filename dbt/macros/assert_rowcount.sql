{% macro test_assert_rowcount(model) %}
  -- Custom test to ensure table has at least one row
  -- Fails if rowcount is 0
  SELECT count(*) as row_count
  FROM {{ model }}
  HAVING count(*) = 0
{% endmacro %}