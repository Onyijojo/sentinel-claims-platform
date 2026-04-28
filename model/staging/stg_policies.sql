-- Staging model: policies. Source defined in src_claims_domain.yml
with source as (
    select * from {{ source('raw_claims', 'policies') }}
),

renamed as (
    select
        policy_id,
        policy_number,
        coverage_type,
        cast(start_date as date)            as start_date,
        cast(end_date   as date)            as end_date,
        cast(premium_amount as numeric(12,2)) as premium_amount,
        cast(created_at as timestamp)       as created_at,
        cast(updated_at as timestamp)       as updated_at
    from source
    where policy_id is not null
)

select * from renamed
