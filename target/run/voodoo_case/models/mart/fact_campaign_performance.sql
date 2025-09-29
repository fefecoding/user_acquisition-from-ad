

  create or replace view `voodoo-473511`.`USER_ACCQUISITION`.`fact_campaign_performance`
  OPTIONS()
  as with metrics as (
    select
        c.campaign_name,
        n.network_name,
        p.platform,
        total_revenue,
        total_users,
        total_spend,
        arpu,
        roas,
        conversion_rate,
        forecasted_revenue,
        high_roas_prob
    from `voodoo-473511`.`USER_ACCQUISITION`.`stg_campaign_metrics` as m
    join `voodoo-473511`.`USER_ACCQUISITION`.`dim_campaign` as c
      on m.campaign_name = c.campaign_name
    join `voodoo-473511`.`USER_ACCQUISITION`.`dim_network` as n
      on m.network_name = n.network_name
    join `voodoo-473511`.`USER_ACCQUISITION`.`dim_platform` as p
      on m.platform = p.platform
)

select
    c.campaign_id,
    n.network_id,
    p.platform_id,
    total_revenue,
    total_users,
    total_spend,
    arpu,
    roas,
    conversion_rate,
    forecasted_revenue,
    high_roas_prob
from metrics m
join `voodoo-473511`.`USER_ACCQUISITION`.`dim_campaign` c on m.campaign_name = c.campaign_name
join `voodoo-473511`.`USER_ACCQUISITION`.`dim_network` n on m.network_name = n.network_name
join `voodoo-473511`.`USER_ACCQUISITION`.`dim_platform` p on m.platform = p.platform;

