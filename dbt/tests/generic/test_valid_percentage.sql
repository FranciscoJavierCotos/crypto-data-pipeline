-- Generic dbt test: asserts a percentage column is within a valid 0–100 range.
-- Usage in schema YAML:
--   columns:
--     - name: btc_dominance_pct
--       tests:
--         - valid_percentage

{% test valid_percentage(model, column_name, tolerance=0.01) %}

select
    {{ column_name }},
    count(*) as violation_count
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < (0 - {{ tolerance }}) or {{ column_name }} > (100 + {{ tolerance }}))
group by {{ column_name }}

{% endtest %}
