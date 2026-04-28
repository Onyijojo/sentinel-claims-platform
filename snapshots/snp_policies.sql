{% snapshot snp_policies %}

{{
    config(
      target_schema='snapshots',
      unique_key='policy_id',
      strategy='timestamp',
      updated_at='updated_at'
    )
}}

select * from {{ ref('stg_policies') }}

{% endsnapshot %}