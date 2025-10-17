{#
    Same as rowcount_match, but tests whether the model has an extra row.
    Needed for candidate and committee, where we import all the raw records, but add an extra default records
    Not needed for seed tables - these have a default record in the csv already.
#}

{% test rowcount_match_with_default(model, compare_to) %}
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
    WHERE model_count.cnt != (compare_count.cnt + 1)
{% endtest %}