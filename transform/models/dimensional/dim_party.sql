{{
  config(
    materialized='table',
    schema='dimensional',
    post_hook=[
      "alter table {{ this }} add primary key (id)"
    ]
  )
}}

with source as (
    select * from {{ ref('party') }}
),

add_surrogate_key as (
    select
        row_number() over (order by code) as id,
        code,
        name,
        notes,
        current_timestamp() as updated_at
        
    from source
)

select * from add_surrogate_key