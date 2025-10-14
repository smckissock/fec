select    
    -- Committee (donating)
    receiving_committee_fec_id as "Committee FEC ID",
    receiving_committee_name as "Committee",
    receiving_committee_state_code as "CommitteeState Code",
    receiving_committee_state as "Committee State",
    receiving_committee_designation as "Committee Designation",
    receiving_committee_type as "Commitee Type",
    receiving_committee_party as "Committee Party",
    
    -- Candidate (if linked)
    coalesce(candidate_fec_id, 'n/a') as "Candidate FEC ID",
    coalesce(candidate_name, 'n/a') as "Candidate",
    coalesce(candidate_party, 'n/a') as "Party",
    coalesce(candidate_office, 'n/a') as "Office",
    coalesce(candidate_office_state_code, 'n/a') as "State Code",
    coalesce(candidate_office_state, 'n/a') as "State",
    coalesce(cast(candidate_election_year as varchar), 'n/a') as "Election Year",
    coalesce(candidate_incumbent_challenger_status, 'n/a') as "Incumbent/Challenger",
    
    -- Contributor geography
    contributor_state_code as "Contributor State Code",
    contributor_state as "Contributor State",

    -- Transaction metadata
    amendment_indicator as "Amendment Indicator",
    report_type as "Report Type",
    transaction_type_code as "Transaction Type Code",
    transaction_type as "Transaction Type",
    entity_type as "Entity Type",
    
    -- Measures
    sum(transaction_amount) as "Total"

from {{ ref('committee_contribution_view') }}
group by 
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

    coalesce(cast(candidate_election_year as varchar), 'n/a'),

    candidate_incumbent_challenger_status,
    contributor_state_code,
    contributor_state,
    amendment_indicator,
    report_type,
    transaction_type_code,
    transaction_type,
    entity_type
