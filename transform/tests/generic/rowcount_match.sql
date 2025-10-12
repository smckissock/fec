{#
  Generic test to verify that two models/tables have the same row count.
  
  This test compares the number of rows in the tested model against another model.
  The test PASSES when both models have identical row counts (returns 0 rows).
  The test FAILS when row counts differ (returns 1 row showing both counts).

  Especially useful for verifying that views with inner joins do not lose rows, 
  since snowflake does not have NOT NULL constraints to enforce this. 
  
  Usage in schema.yaml:
    models:
      - name: my_model
        tests:
          - rowcount_match:
              compare_to: ref('other_model')
  
  Example:
    - name: dim_candidate_view
      tests:
        - rowcount_match:
            compare_to: ref('dim_candidate')
#}

{% test rowcount_match(model, compare_to) %}
    WITH model_count AS (
        SELECT COUNT(*) AS cnt FROM {{ model }}
    ),
    compare_count AS (
        SELECT COUNT(*) AS cnt FROM {{ compare_to }}
    )
    
    SELECT 
        model_count.cnt AS model_rows,
        compare_count.cnt AS compare_rows
    FROM model_count
    CROSS JOIN compare_count
    WHERE model_count.cnt != compare_count.cnt
{% endtest %}