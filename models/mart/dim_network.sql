with networks as (
    select distinct
        network_name
    from {{ ref('stg_campaign_metrics') }}
)

select
    row_number() over() as network_id,
    network_name
from networks
