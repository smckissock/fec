{{
    config(
        materialized='view',
        schema='dimensional'
    )
}}

select
    f.sub_id,
    f.transaction_id,
    f.file_number,
    f.image_number,
    
    -- Transaction details
    f.transaction_date,
    f.transaction_amount,
    
    -- Committee (receiving)
    f.committee_id as receiving_committee_id,
    cmte.cmte_id as receiving_committee_fec_id,
    cmte.committee_name as receiving_committee_name,
    cmte.treasurer_name as receiving_committee_treasurer,
    cmte_st.code as receiving_committee_state_code,
    cmte_st.name as receiving_committee_state,
    cmte_desg.code as receiving_committee_designation_code,
    cmte_desg.name as receiving_committee_designation,
    cmte_type.code as receiving_committee_type_code,
    cmte_type.name as receiving_committee_type,
    cmte_party.code as receiving_committee_party_code,
    cmte_party.name as receiving_committee_party,
    
    -- Candidate (if linked to receiving committee)
    cand.cand_id as candidate_fec_id,
    cand.cand_name as candidate_name,
    cand_party.code as candidate_party_code,
    cand_party.name as candidate_party,
    cand_office.name as candidate_office,
    cand_office_st.code as candidate_office_state_code,
    cand_office_st.name as candidate_office_state,
    cand.office_district as candidate_office_district,
    cand.election_year as candidate_election_year,
    cand_ici.name as candidate_incumbent_challenger_status,
    
    -- Contributor information
    f.contributor_name,
    f.contributor_city,
    contrib_st.code as contributor_state_code,
    contrib_st.name as contributor_state,
    f.contributor_zip_code,
    f.contributor_employer,
    f.contributor_occupation,
    
    -- Transaction metadata
    amend.code as amendment_indicator_code,
    amend.name as amendment_indicator,
    rpt.code as report_type_code,
    rpt.name as report_type,
    pgi.code as primary_general_indicator_code,
    pgi.name as primary_general_indicator,
    trans_type.code as transaction_type_code,
    trans_type.description as transaction_type,
    entity.code as entity_type_code,
    entity.name as entity_type,
    
    -- Other fields
    f.other_id,
    f.memo_code,
    f.memo_text

from {{ ref('fact_committee_contribution') }} f
inner join {{ ref('dim_committee') }} cmte on f.committee_id = cmte.id
inner join {{ ref('dim_state') }} cmte_st on cmte.state_id = cmte_st.id
inner join {{ ref('dim_committee_designation') }} cmte_desg on cmte.committee_designation_id = cmte_desg.id
inner join {{ ref('dim_committee_type') }} cmte_type on cmte.committee_type_id = cmte_type.id
inner join {{ ref('dim_party') }} cmte_party on cmte.party_id = cmte_party.id
left join {{ ref('dim_candidate') }} cand on cmte.candidate_id = cand.id
left join {{ ref('dim_party') }} cand_party on cand.party_id = cand_party.id
left join {{ ref('dim_office') }} cand_office on cand.office_id = cand_office.id
left join {{ ref('dim_state') }} cand_office_st on cand.office_state_id = cand_office_st.id
left join {{ ref('dim_incumbent_challenger_status') }} cand_ici on cand.incumbent_challenger_status_id = cand_ici.id
inner join {{ ref('dim_state') }} contrib_st on f.state_id = contrib_st.id
inner join {{ ref('dim_ammendment_code') }} amend on f.amendment_indicator_id = amend.id
inner join {{ ref('dim_report_type') }} rpt on f.report_type_id = rpt.id
inner join {{ ref('dim_primary_general_indicator') }} pgi on f.primary_general_indicator_id = pgi.id
inner join {{ ref('dim_transaction_type') }} trans_type on f.transaction_type_id = trans_type.id
inner join {{ ref('dim_entity_type') }} entity on f.entity_type_id = entity.id