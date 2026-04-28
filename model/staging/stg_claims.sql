-- Staging model: claims (uses claims_v2 — supersedes v1 with claim_severity).
-- Source defined in src_claims_domain.yml
with source as (
    select * from {{ source('raw_claims', 'claims') }}
),

renamed as (
    select
        claim_id,
        claimant_id,
        cast(policy_id as integer)              as policy_id,
        cast(incident_date as date)             as incident_date,
        cast(report_date   as date)             as report_date,
        -- Downstream fact joins on claim_date; alias incident_date here so the contract is explicit
        cast(incident_date as date)             as claim_date,
        claim_type,
        upper(claim_status)                     as claim_status,
        cast(claim_amount    as numeric(14,2))  as claim_amount,
        cast(approved_amount as numeric(14,2))  as approved_amount,
        claim_severity,
        cast(created_at as timestamp)           as created_at,
        cast(updated_at as timestamp)           as updated_at
    from source
    where claim_id is not null
)

select * from renamed
