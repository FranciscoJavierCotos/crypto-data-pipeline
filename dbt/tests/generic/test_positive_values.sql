-- Generic dbt test: asserts that a numeric column has no negative values.
-- Usage in schema YAML:
--   columns:
--     - name: current_price
--       tests:
--         - positive_values

{% test positive_values(model, column_name) %}

select
    {{ column_name }},
    count(*) as violation_count
from {{ model }}
where {{ column_name }} is not null
  and {{ column_name }} < 0
group by {{ column_name }}

{% endtest %}
