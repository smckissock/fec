{{
    config(
        materialized='view',
        schema='analytics'
    )
}}

select
    -- Transaction date attributes
    year(transaction_date) as transaction_year,
    
    -- Committee (receiving)
    receiving_committee_fec_id,
    receiving_committee_name,
    receiving_committee_state_code,
    receiving_committee_state,
    receiving_committee_designation,
    receiving_committee_type,
    receiving_committee_party,
    
    -- Candidate (if linked)
    candidate_fec_id,
    candidate_name,
    candidate_party,
    candidate_office,
    candidate_office_state_code,
    candidate_office_state,
    candidate_election_year,
    candidate_incumbent_challenger_status,
    
    -- Contributor geography
    contributor_state_code,
    contributor_state,
    
    -- Transaction metadata
    amendment_indicator,
    report_type,
    transaction_type_code,
    transaction_type,
    entity_type,
    
    -- Aggregated measures
    count(*) as transaction_count,
    sum(transaction_amount) as total_amount

from {{ ref('committee_contribution_view') }}
group by 
    transaction_year,
    receiving_committee_fec_id,
    receiving_committee_name,
    receiving_committee_state_code,
    receiving_committee_state,
    receiving_committee_designation,
    receiving_committee_type,
    receiving_committee_party,
    candidate_fec_id,
    candidate_name,
    candidate_party,
    candidate_office,
    candidate_office_state_code,
    candidate_office_state,
    candidate_election_year,
    candidate_incumbent_challenger_status,
    contributor_state_code,
    contributor_state,
    amendment_indicator,
    report_type,
    transaction_type_code,
    transaction_type,
    entity_type