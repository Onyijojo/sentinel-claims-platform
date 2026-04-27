{{
  config(
    materialized='incremental',
    unique_key='user_id'
  )
}}

select * from renamed
{% if is_incremental() %}
  -- only look at files added since the last dbt run
  where dbt_updated_at > (select max(dbt_updated_at) from {{ this }})
{% endif %}

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