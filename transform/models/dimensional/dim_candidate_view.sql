{{
    config(
        materialized='view',
        schema='dimensional'
    )
}}

select
    c.id,
    c.cand_id,
    c.cand_name,
    
    -- Dimension attributes (not IDs)
    p.code as party_code,
    p.name as party,
    o.name as office,
    os.code as office_state_code,
    os.name as office_state,
    cs.name as candidate_status,
    ici.name as incumbent_challenger_status,
    
    -- Candidate attributes
    c.election_year,
    c.office_district,
    c.principal_campaign_committee,
    
    -- Mailing address
    c.street_1,
    c.street_2,
    c.city,
    ms.code as mailing_state_code,
    ms.name as mailing_state,
    c.zip_code

from {{ ref('dim_candidate') }} c
inner join {{ ref('dim_party') }} p on c.party_id = p.id
inner join {{ ref('dim_office') }} o on c.office_id = o.id
inner join {{ ref('dim_state') }} os on c.office_state_id = os.id
inner join {{ ref('dim_candidate_status') }} cs on c.candidate_status_id = cs.id
inner join {{ ref('dim_incumbent_challenger_status') }} ici on c.incumbent_challenger_status_id = ici.id
inner join {{ ref('dim_state') }} ms on c.mailing_state_id = ms.id