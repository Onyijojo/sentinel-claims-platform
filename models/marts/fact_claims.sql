with claims as (
    select * from {{ ref('stg_claims') }}
),

claimants as (
    select * from {{ ref('dim_claimants') }}
),

policies as (
    select * from {{ ref('dim_policies') }}
),

final as (
    select
        -- Generate a unique Surrogate Key for the claim itself
        {{ dbt_utils.generate_surrogate_key(['claims.claim_id']) }} as claim_sk,
        
        -- Foreign Keys to Dimensions
        claimants.claimant_sk,
        policies.policy_sk,
        
        -- Fact Columns (Measures & Degenerate Dimensions)
        claims.claim_id,
        claims.claim_date,
        claims.claim_amount,
        claims.claim_status,
        
        -- Audit Column
        current_timestamp as loaded_at
        
    from claims
    -- Join to Claimants: Match the ID AND ensure the claim date falls within the SCD Type 2 timeframe
    left join claimants 
        on claims.claimant_id = claimants.claimant_id
        and claims.claim_date >= claimants.valid_from
        and (claims.claim_date < claimants.valid_to or claimants.valid_to is null)
        
    -- Join to Policies: Same temporal join logic
    left join policies 
        on claims.policy_id = policies.policy_id
        and claims.claim_date >= policies.valid_from
        and (claims.claim_date < policies.valid_to or policies.valid_to is null)
)

select * from final

-- Execution
-- To build this entire pipeline in dbt Cloud, you will run your commands in this order:

-- dbt run -s stg_claimants stg_policies stg_claims (Builds staging)

-- dbt snapshot (Captures the current state of history)

-- dbt run -s dim_claimants dim_policies fact_claims (Builds the star schema)