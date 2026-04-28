-- Staging model: pulls from the canonical raw source defined in src_google_drive.yml
with source as (
    select * from {{ source('raw_data_source', 'drive_csv_data') }}
),

renamed as (
    select
        id                              as user_id,
        upper(status)                   as enrollment_status,
        cast(created_at as timestamp)   as created_at,
        current_timestamp               as dbt_updated_at
    from source
)

select * from renamed