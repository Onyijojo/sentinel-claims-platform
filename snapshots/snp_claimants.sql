{% snapshot snp_claimants %}

{{
    config(
      target_schema='snapshots',
      unique_key='claimant_id',
      strategy='timestamp',
      updated_at='updated_at' -- The column in your staging table that shows when the row was last modified
    )
}}

select * from {{ ref('stg_claimants') }}

{% endsnapshot %}