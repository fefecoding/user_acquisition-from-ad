select
    platform,
    campaign_name,
    network_name,
    total_revenue,
    total_users,
    conversion_rate,
    total_spend,
    roas,
    arpu,
    forecasted_revenue,
    high_roas,
    high_roas_prob
from {{ ref('campaign_analysis') }}