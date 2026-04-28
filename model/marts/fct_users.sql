-- Fact model: user dimension with derived signup month, sourced from stg_data
with staging_data as (
    -- Use ref() to point at the staging model (never the raw table)
    select * from {{ ref('stg_data') }}
),

final as (
    select
        user_id,
        email,
        created_at,
        date_trunc('month', created_at) as signup_month,
        dbt_updated_at
    from staging_data
    where user_id is not null
)

select * from final