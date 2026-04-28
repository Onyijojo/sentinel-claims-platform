-- Fact model: enrollment data per user, sourced from stg_drive_data
with staging_data as (
    -- Use ref() to point at the staging model (never the raw table)
    select * from {{ ref('stg_drive_data') }}
),

final as (
    select
        user_id,
        signup_date,
        enrollment_status,
        -- Status flags (mutually exclusive)
        case when enrollment_status = 'ACTIVE'   then 1 else 0 end as is_active,
        case when enrollment_status = 'INACTIVE' then 1 else 0 end as is_inactive,
        case when enrollment_status = 'PENDING'  then 1 else 0 end as is_pending,
        -- Audit column propagated from staging
        dbt_updated_at
    from staging_data
    where user_id is not null
)

select * from final