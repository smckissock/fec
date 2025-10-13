{{
    config(
        materialized='view',
        schema='dimensional'
    )
}}

select
    c.id,
    c.cmte_id,
    c.committee_name,
    c.treasurer_name,
    
    st.code as state_code,
    st.name as state,
    cd.code as committee_designation_code,
    cd.name as committee_designation,
    ct.code as committee_type_code,
    ct.name as committee_type,
    ct.explanation as committee_type_explanation,
    p.code as party_code,
    p.name as party,
    et.code as entity_type_code,
    et.name as entity_type,
    ff.code as filing_frequency_code,
    ff.name as filing_frequency,
    igc.code as interest_group_category_code,
    igc.name as interest_group_category,
    
    -- Linked candidate
    cand.cand_id,
    cand.cand_name as candidate_name,
    
    -- Committee attributes
    c.street_1,
    c.street_2,
    c.city,
    c.zip_code,
    c.connected_organization_name

from {{ ref('dim_committee') }} c
inner join {{ ref('dim_state') }} st on c.state_id = st.id
inner join {{ ref('dim_committee_designation') }} cd on c.committee_designation_id = cd.id
inner join {{ ref('dim_committee_type') }} ct on c.committee_type_id = ct.id
inner join {{ ref('dim_party') }} p on c.party_id = p.id
inner join {{ ref('dim_entity_type') }} et on c.entity_type_id = et.id
inner join {{ ref('dim_committee_filing_frequency') }} ff on c.filing_frequency_id = ff.id
inner join {{ ref('dim_interest_group_category') }} igc on c.interest_group_category_id = igc.id
left join {{ ref('dim_candidate') }} cand on c.candidate_id = cand.id