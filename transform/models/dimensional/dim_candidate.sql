{{
    config(
        materialized='table',
        schema='dimensional',
        unique_key='id'
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

-- Add a default record with ID 1
default_record as (
    select 
        1 as id,
        'N/A' as cand_id,
        'Not Applicable' as cand_name,
        1 as party_id,
        1 as office_id,
        1 as office_state_id,
        1 as candidate_status_id,
        1 as incumbent_challenger_status_id,
        1 as mailing_state_id,
        '' as election_year,
        '' as office_district,
        '' as principal_campaign_committee,
        '' as street_1,
        '' as street_2,
        '' as city,
        '' as zip_code,
        '' as source_file_year
),

transformed as (
    select
        row_number() over (order by s.cand_id) + 1 as id,  -- Start from 2 to leave 1 for default record
        s.cand_id as cand_id,
        s.cand_name,
        
        -- Foreign keys to dimensional tables (default to ID=1 for unknown/missing values)
        coalesce(p.id, 1) as party_id,
        coalesce(o.id, 1) as office_id,
        coalesce(os.id, 1) as office_state_id,
        coalesce(cs.id, 1) as candidate_status_id,
        coalesce(ici.id, 1) as incumbent_challenger_status_id,
        coalesce(st.id, 1) as mailing_state_id,
        
        -- Descriptive attributes with coalesce to prevent null values
        coalesce(s.cand_election_yr, '') as election_year,
        coalesce(s.cand_office_district, '') as office_district,
        coalesce(s.cand_pcc, '') as principal_campaign_committee,
        coalesce(s.cand_st1, '') as street_1,
        coalesce(s.cand_st2, '') as street_2,
        coalesce(s.cand_city, '') as city,
        coalesce(s.cand_zip, '') as zip_code,
        coalesce(s.source_file_year, '') as source_file_year
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
),

-- Union the default record with the transformed records
final as (
    select * from default_record
    union all
    select * from transformed
)

select * from final