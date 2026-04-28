-- Staging model: pulls from the canonical raw source defined in src_google_drive.yml
with source as (
    -- ref('source') links to the source defined in src_google_drive.yml
    select * from {{ source('raw_data_source', 'drive_csv_data') }}
),

renamed as (
    select
        -- Rename raw CSV headers to clean snake_case
        id                              as user_id,
        cast(signup_date as date)       as signup_date,
        upper(status)                   as enrollment_status,
        -- Metadata column to track when dbt processed this row
        current_timestamp               as dbt_updated_at
    from source
    where id is not null
)

select * from renamed