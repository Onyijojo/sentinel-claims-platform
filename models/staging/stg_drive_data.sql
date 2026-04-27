-- Rename the 'raw_data_source' and 'drive_csv_data' to match your actual source and table names in Redshift
with source as (
    -- This macro links to the source defined in step 1
    select * from {{ source('raw_data_source', 'drive_csv_data') }}
),

renamed as (
    select
        -- Rename ugly CSV headers to clean snake_case
        id as user_id,
        cast(signup_date as date) as signup_date,
        upper(status) as enrollment_status,
        -- Add a metadata column to track when dbt processed this
        current_timestamp as dbt_updated_at
    from source
)

select * from renamed