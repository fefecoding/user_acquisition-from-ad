

  create or replace view `voodoo-473511`.`USER_ACCQUISITION`.`stg_campaign_metrics`
  OPTIONS()
  as select
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
from `voodoo-473511`.`USER_ACCQUISITION`.`campaign_analysis`;

