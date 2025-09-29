with campaigns as (
    select distinct
        campaign_name
    from {{ ref('stg_campaign_metrics') }}
)

select
    row_number() over() as campaign_id,
    campaign_name
from campaigns
