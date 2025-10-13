{{
    config(
        materialized='table',
        schema='dimensional'
    )
}}

with source as (
    select * from {{ source('raw', 'individual_contribution') }}
),

dim_committee as (
    select * from {{ ref('dim_committee') }}
),

dim_state as (
    select * from {{ ref('dim_state') }}
),

dim_ammendment_code as (
    select * from {{ ref('dim_ammendment_code') }}
),

dim_report_type as (
    select * from {{ ref('dim_report_type') }}
),

dim_primary_general_indicator as (
    select * from {{ ref('dim_primary_general_indicator') }}
),

dim_transaction_type as (
    select * from {{ ref('dim_transaction_type') }}
),

dim_entity_type as (
    select * from {{ ref('dim_entity_type') }}
),

transformed as (
    select
        row_number() over (order by s.sub_id) as id,
        s.sub_id,
        s.tran_id as transaction_id,
        s.file_num as file_number,
        s.image_num as image_number,
        
        -- Foreign keys to dimensional tables (default to ID=1 for unknown/missing values)
        coalesce(cmte.id, 1) as committee_id,
        coalesce(st.id, 1) as state_id,
        coalesce(ac.id, 1) as amendment_indicator_id,
        coalesce(rt.id, 1) as report_type_id,
        coalesce(pgi.id, 1) as primary_general_indicator_id,
        coalesce(tt.id, 1) as transaction_type_id,
        coalesce(et.id, 1) as entity_type_id,
        
        -- Transaction details
        try_to_date(s.transaction_dt, 'MMDDYYYY') as transaction_date,
        try_to_number(s.transaction_amt, 10, 2) as transaction_amount,
        
        -- Contributor information
        s.name as contributor_name,
        s.city as contributor_city,
        s.zip_code as contributor_zip_code,
        s.employer as contributor_employer,
        s.occupation as contributor_occupation,
        
        -- Other fields
        s.other_id,
        s.memo_cd as memo_code,
        s.memo_text
        
    from source s
    left join dim_committee cmte on s.cmte_id = cmte.cmte_id
    left join dim_state st on s.state = st.code
    left join dim_ammendment_code ac on s.amndt_ind = ac.code
    left join dim_report_type rt on s.rpt_tp = rt.code
    left join dim_primary_general_indicator pgi on s.transaction_pgi = pgi.code
    left join dim_transaction_type tt on s.transaction_tp = tt.code
    left join dim_entity_type et on s.entity_tp = et.code
)

select * from transformed