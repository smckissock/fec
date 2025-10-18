{# 
This test ensures a unique combination across specified columns.

It identifies any combination of the specified columns that appears 
more than once in the dataset, which would violate a composite unique constraint.

Example: For candidate records, ensures no duplicate 
(candidate_id, source_file_year) combinations exist

Example Usage in schema.yaml:
models:
  - name: dim_candidate
    columns:
      - name: cand_id
        tests:
          # Ensures no duplicate combinations of cand_id and source_file_year
          - unique_combination:
              column_names: ['cand_id', 'source_file_year']

  # Another example: Ensure unique committee identification across years
  - name: dim_committee
    columns:
      - name: cmte_id
        tests:
          - unique_combination:
              column_names: ['cmte_id', 'source_file_year']
#}

{% test unique_combination(model, column_names) %}
with validation as (

    select
        {% for column in column_names %}
        {{ column }},
        {% endfor %}
        count(*) as n_records

    from {{ model }}
    group by 
        {% for column in column_names %}
        {{ column }}{% if not loop.last %},{% endif %}
        {% endfor %}
    having count(*) > 1

)

select *
from validation

{% endtest %}