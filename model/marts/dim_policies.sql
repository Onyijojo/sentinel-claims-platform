-- Dimension: policies (SCD Type 2, sourced from snp_policies)
-- Requires dbt_utils (declared in packages.yml at the project root).
with snapshot_data as (
    select * from {{ ref('snp_policies') }}
),

final as (
    select
        -- Surrogate key: policy_id + valid_from makes each historical version unique
        {{ dbt_utils.generate_surrogate_key(['policy_id', 'dbt_valid_from']) }} as policy_sk,

        -- Natural key + attributes (columns aligned to policies.csv)
        policy_id,
        policy_number,
        coverage_type,
        premium_amount,
        start_date,
        end_date,

        -- SCD Type 2 audit / history columns
        dbt_valid_from                              as valid_from,
        dbt_valid_to                                as valid_to,
        case when dbt_valid_to is null then true else false end as is_current_record

    from snapshot_data
)

select * from final