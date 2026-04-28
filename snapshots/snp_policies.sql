{% snapshot snp_policies %}

{{
    config(
      target_schema='snapshots',
      unique_key='policy_id',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True
    )
}}

-- Captures SCD Type 2 history of policies. Strategy uses stg_policies.updated_at
-- to detect changes; rows present in a prior run but missing now are invalidated.
select * from {{ ref('stg_policies') }}

{% endsnapshot %}