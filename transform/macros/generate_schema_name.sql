{# 
This macro overrides dbt's default schema naming behavior.

By default, dbt appends custom schema names to the target schema from profiles.yml.
For example, with target schema 'raw' and model schema 'normalized', 
dbt would create: raw_normalized

This macro removes that behavior and uses the exact schema name specified 
in the model configuration, so 'normalized' creates schema 'NORMALIZED' directly.

This is useful when you want:
- Exact control over schema names (NORMALIZED, DIMENSIONAL, ANALYTICS)
- To use pre-existing schemas without prefixes
- Schema names that match your naming conventions exactly

Without this macro: FEC.RAW_NORMALIZED.candidate
With this macro: FEC.NORMALIZED.candidate
#}


{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}