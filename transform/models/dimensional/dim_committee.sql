{{
    config(
        materialized='table',
        schema='dimensional'
    )
}}

with source as (
    select * from {{ source('raw', 'committee') }}
),

dim_state as (
    select * from {{ ref('dim_state') }}
),

dim_party as (
    select * from {{ ref('dim_party') }}
),

dim_committee_designation as (
    select * from {{ ref('dim_committee_designation') }}
),

dim_committee_type as (
    select * from {{ ref('dim_committee_type') }}
),

dim_entity_type as (
    select * from {{ ref('dim_entity_type') }}
),

dim_committee_filing_frequency as (
    select * from {{ ref('dim_committee_filing_frequency') }}
),

dim_interest_group_category as (
    select * from {{ ref('dim_interest_group_category') }}
),

dim_candidate as (
    select * from {{ ref('dim_candidate') }}
),

transformed as (
    select
        row_number() over (order by s.cmte_id) as id,
        s.cmte_id,
        coalesce(s.cmte_nm, 'Unspecified') as committee_name,
        s.tres_nm as treasurer_name,
        
        -- Foreign keys to dimensional tables (default to ID=1 for unknown/missing values)
        coalesce(st.id, 1) as state_id,
        coalesce(cd.id, 1) as committee_designation_id,
        coalesce(ct.id, 1) as committee_type_id,
        coalesce(p.id, 1) as party_id,
        coalesce(et.id, 1) as entity_type_id,
        coalesce(ff.id, 1) as filing_frequency_id,
        coalesce(igc.id, 1) as interest_group_category_id,
        cand.id as candidate_id,
        
        -- Committee attributes
        s.cmte_st1 as street_1,
        s.cmte_st2 as street_2,
        s.cmte_city as city,
        s.cmte_zip as zip_code,
        s.connected_org_nm as connected_organization_name
        
    from source s
    left join dim_state st on s.cmte_st = st.code
    left join dim_committee_designation cd on s.cmte_dsgn = cd.code
    left join dim_committee_type ct on s.cmte_tp = ct.code
    left join dim_party p on s.cmte_pty_affiliation = p.code
    left join dim_entity_type et on s.org_tp = et.code
    left join dim_committee_filing_frequency ff on s.cmte_filing_freq = ff.code
    left join dim_interest_group_category igc on s.org_tp = igc.code
    left join dim_candidate cand on s.cand_id = cand.cand_id
)

select * from transformed