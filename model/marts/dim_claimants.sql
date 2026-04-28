-- Dimension: claimants (SCD Type 2, sourced from snp_claimants)
-- Requires dbt_utils (declared in packages.yml at the project root).
with snapshot_data as (
    select * from {{ ref('snp_claimants') }}
),

final as (
    select
        -- Surrogate key: claimant_id + valid_from makes each historical version unique
        {{ dbt_utils.generate_surrogate_key(['claimant_id', 'dbt_valid_from']) }} as claimant_sk,

        -- Natural key + attributes (columns aligned to claimants.csv)
        claimant_id,
        first_name,
        last_name,
        date_of_birth,
        gender,
        employment_start_date,
        employer_id,

        -- SCD Type 2 audit / history columns
        dbt_valid_from                              as valid_from,
        dbt_valid_to                                as valid_to,
        case when dbt_valid_to is null then true else false end as is_current_record

    from snapshot_data
)

select * from final