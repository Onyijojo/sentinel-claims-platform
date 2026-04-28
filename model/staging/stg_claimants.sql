-- Staging model: claimants. Source defined in src_claims_domain.yml
with source as (
    select * from {{ source('raw_claims', 'claimants') }}
),

renamed as (
    select
        claimant_id,
        first_name,
        last_name,
        cast(date_of_birth as date)             as date_of_birth,
        gender,
        cast(employment_start_date as date)     as employment_start_date,
        employer_id,
        cast(created_at as timestamp)           as created_at,
        cast(updated_at as timestamp)           as updated_at
    from source
    where claimant_id is not null
)

select * from renamed
