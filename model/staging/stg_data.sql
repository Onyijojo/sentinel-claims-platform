-- Staging model: pulls from the canonical raw source defined in src_google_drive.yml
with source as (
    select * from {{ source('raw_data_source', 'drive_csv_data') }}
),

renamed as (
    select
        id                                      as user_id,
        cast(signup_timestamp as timestamp)     as created_at,
        lower(email_address)                    as email,
        current_timestamp                       as dbt_updated_at
    from source
    where id is not null
)

select * from renamed