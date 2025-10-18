{{
    config(
        materialized='table',
        schema='dimensional'
    )
}}

with source as (
    select * from {{ source('raw', 'committee_contribution') }}
),

dim_committee as (
    select * from {{ ref('dim_committee') }}
),

dim_candidate as (
    select * from {{ ref('dim_candidate') }}
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
        coalesce(s.sub_id, '') as sub_id, -- Should be unique, but has nulls
        coalesce(s.tran_id, '') as transaction_id,
        coalesce(s.file_num, '') as file_number,
        coalesce(s.image_num, '') as image_number,
        
        -- Foreign keys to dimensional tables (default to ID=1 for unknown/missing values)
        coalesce(cmte.id, 1) as committee_id,
        coalesce(cand.id, 1) as candidate_id,
        coalesce(st.id, 1) as state_id,
        coalesce(ac.id, 1) as amendment_indicator_id,
        coalesce(rt.id, 1) as report_type_id,
        coalesce(pgi.id, 1) as primary_general_indicator_id,
        coalesce(tt.id, 1) as transaction_type_id,
        coalesce(et.id, 1) as entity_type_id,
        
        -- Transaction details
        try_to_date(s.transaction_dt, 'MMDDYYYY') as transaction_date,
        coalesce(try_to_number(s.transaction_amt, 10, 2), 0) as transaction_amount,
        
        -- Contributor information
        coalesce(s.name, '') as contributor_name,
        coalesce(s.city, '') as contributor_city,
        coalesce(s.zip_code, '') as contributor_zip_code,
        coalesce(s.employer, '') as contributor_employer,
        coalesce(s.occupation, '') as contributor_occupation,
        
        -- Other fields
        coalesce(s.other_id, '') as other_id,
        coalesce(s.memo_cd, '') as memo_code,
        coalesce(s.memo_text, '') as memo_text,
        coalesce(s.source_file_year, '') as source_file_year
        
    from source s
    -- JOIN MUST INCLUDE SOURCE FILE YEAR, OR IT WILL JOIN FOR EACH CYCLE!!
    left join dim_committee cmte 
        on s.cmte_id = cmte.cmte_id 
        and coalesce(s.source_file_year, '') = coalesce(cmte.source_file_year, '')
    -- JOIN MUST INCLUDE SOURCE FILE YEAR, OR IT WILL JOIN FOR EACH CYCLE!!
    left join dim_candidate cand 
        on s.cand_id = cand.cand_id 
        and coalesce(s.source_file_year, '') = coalesce(cand.source_file_year, '')
    left join dim_state st on s.state = st.code
    left join dim_ammendment_code ac on s.amndt_ind = ac.code
    left join dim_report_type rt on s.rpt_tp = rt.code
    left join dim_primary_general_indicator pgi on s.transaction_pgi = pgi.code
    left join dim_transaction_type tt on s.transaction_tp = tt.code
    left join dim_entity_type et on s.entity_tp = et.code
)

select * from transformed