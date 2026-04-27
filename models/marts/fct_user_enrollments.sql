with staging_data as (
    -- Use 'ref' to point to your staging model, not the raw table
    select * from {{ ref('stg_drive_data') }}
),

final as (
    select
        user_id, -- Assuming there's a user_id in the staging data
        signup_date, -- Assuming there's a signup_date in the staging data
        enrollment_status, -- Assuming there's an enrollment_status in the staging data
        -- Example logic: flag active users
        case when enrollment_status = 'ACTIVE' then 1 else 0 end as is_active
    from staging_data
)

select * from final