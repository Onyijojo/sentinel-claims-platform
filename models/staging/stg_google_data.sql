-- Replace 'raw_zone' with location of the raw data
with source as (
    select * from {{ source('raw_zone', 'my_csv_table') }}
),
renamed as (
    select
        id as user_id,
        upper(status) as enrollment_status,
        cast(created_at as timestamp) as created_at
    from source
)
select * from renamed