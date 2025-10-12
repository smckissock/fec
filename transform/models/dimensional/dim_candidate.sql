{{
    config(
        materialized='table',
        schema='dimensional'
    )
}}

with source as (
    select * from {{ source('raw', 'candidate') }}
),

dim_state as (
    select * from {{ ref('dim_state') }}
),

dim_party as (
    select * from {{ ref('dim_party') }}
),

dim_office as (
    select * from {{ ref('dim_office') }}
),

dim_candidate_status as (
    select * from {{ ref('dim_candidate_status') }}
),

dim_incumbent_challenger_status as (
    select * from {{ ref('dim_incumbent_challenger_status') }}
),

transformed as (
    select
        row_number() over (order by s.cand_id) as id,
        s.cand_id as cand_id,
        s.cand_name,
        
        -- Foreign keys to dimensional tables (default to ID=1 for unknown/missing values)
        coalesce(p.id, 1) as party_id,
        coalesce(o.id, 1) as office_id,
        coalesce(os.id, 1) as office_state_id,
        coalesce(cs.id, 1) as candidate_status_id,
        coalesce(ici.id, 1) as incumbent_challenger_status_id,
        coalesce(st.id, 1) as mailing_state_id,
        
        -- Descriptive attributes
        s.cand_election_yr as election_year,
        s.cand_office_district as office_district,
        s.cand_pcc as principal_campaign_committee,
        s.cand_st1 as street_1,
        s.cand_st2 as street_2,
        s.cand_city as city,
        s.cand_zip as zip_code
    from source s
    left join dim_party p 
        on s.cand_pty_affiliation = p.code
    left join dim_office o 
        on s.cand_office = o.code
    left join dim_state os 
        on s.cand_office_st = os.code
    left join dim_candidate_status cs 
        on s.cand_status = cs.code
    left join dim_incumbent_challenger_status ici 
        on s.cand_ici = ici.code
    left join dim_state st 
        on s.cand_st = st.code
)

select * from transformed