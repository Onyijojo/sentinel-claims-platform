with staging as (
    select * from {{ ref('stg_data') }} -- This creates the link in the graph
)
select 
    *,
    date_trunc('month', created_at) as joining_month
from staging