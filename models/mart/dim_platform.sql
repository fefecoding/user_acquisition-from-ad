with platforms as (
    select distinct
        platform
    from {{ ref('stg_campaign_metrics') }}
)

select
    row_number() over() as platform_id,
    platform
from platforms
