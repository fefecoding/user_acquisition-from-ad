with platforms as (
    select distinct
        platform
    from {{ ref('stg_campaign_metrics') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['platform']) }} as platform_id,
    platform,
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
from platforms