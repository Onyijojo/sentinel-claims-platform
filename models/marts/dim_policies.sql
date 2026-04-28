with snapshot_data as (
    select * from {{ ref('snp_policies') }}
),

final as (
    select
        -- Generate a Surrogate Key combining the ID and the valid_from date
        {{ dbt_utils.generate_surrogate_key(['policy_id', 'dbt_valid_from']) }} as policy_sk,
        policy_id,
        coverage_amount,
        premium,
        start_date,
        end_date,
        
        -- Audit & History Columns
        dbt_valid_from as valid_from,
        dbt_valid_to as valid_to,
        case 
            when dbt_valid_to is null then true 
            else false 
        end as is_current_record
        
    from snapshot_data
)

select * from final