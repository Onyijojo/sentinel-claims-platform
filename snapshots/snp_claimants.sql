{% snapshot snp_claimants %}

{{
    config(
      target_schema='snapshots',
      unique_key='claimant_id',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True
    )
}}

-- Captures SCD Type 2 history of claimants. Strategy uses stg_claimants.updated_at
-- to detect changes; rows present in a prior run but missing now are invalidated.
select * from {{ ref('stg_claimants') }}

{% endsnapshot %}