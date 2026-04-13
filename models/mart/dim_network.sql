with networks as (
    select distinct
        network_name
    from {{ ref('stg_campaign_metrics') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['network_name']) }} as network_id,
    network_name,
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
from networks