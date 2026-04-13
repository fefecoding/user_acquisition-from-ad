with campaigns as (
    select distinct
        campaign_name
    from {{ ref('stg_campaign_metrics') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['campaign_name']) }} as campaign_id,
    campaign_name,
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
from campaigns