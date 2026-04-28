-- Fact: claims, joined to claimant and policy dimensions using SCD Type 2 temporal joins.
-- Requires dbt_utils (declared in packages.yml at the project root).
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
        -- Surrogate key for the fact row
        {{ dbt_utils.generate_surrogate_key(['claim_id']) }} as claim_sk,

        -- Foreign keys to dimensions (point-in-time correct via temporal join below)
        claimants.claimant_sk,
        policies.policy_sk,

        -- Degenerate dimensions
        claims.claim_id,
        claims.claimant_id,
        claims.policy_id,
        claims.claim_type,
        claims.claim_status,
        claims.claim_severity,

        -- Dates
        claims.incident_date,
        claims.report_date,
        claims.claim_date,

        -- Measures
        claims.claim_amount,
        claims.approved_amount,

        -- Audit
        current_timestamp as loaded_at

    from claims
    -- SCD Type 2 join: pick the version of the claimant valid on the claim date
    left join claimants
        on  claims.claimant_id = claimants.claimant_id
        and claims.claim_date >= claimants.valid_from
        and (claims.claim_date < claimants.valid_to or claimants.valid_to is null)

    -- SCD Type 2 join: pick the version of the policy valid on the claim date
    left join policies
        on  claims.policy_id = policies.policy_id
        and claims.claim_date >= policies.valid_from
        and (claims.claim_date < policies.valid_to or policies.valid_to is null)
)

select * from final

-- ----------------------------------------------------------------------------
-- Build order:
--   dbt deps                                                    (install dbt_utils)
--   dbt run -s stg_claimants stg_policies stg_claims            (staging)
--   dbt snapshot                                                (history capture)
--   dbt run -s dim_claimants dim_policies fact_claims           (star schema)
-- Or simply:
--   dbt build
-- ----------------------------------------------------------------------------