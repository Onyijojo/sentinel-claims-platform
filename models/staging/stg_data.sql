-- Rename 'google_drive_raw.your_table_name' to your actual source table name
with source as (
    select * from {{ source('google_drive_raw', 'your_table_name') }}
),
renamed as (
    select
        id as user_id,
        cast(signup_timestamp as timestamp) as created_at,
        lower(email_address) as email
    from source
)
select * from renamed