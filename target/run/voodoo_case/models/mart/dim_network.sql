

  create or replace view `voodoo-473511`.`USER_ACCQUISITION`.`dim_network`
  OPTIONS()
  as with networks as (
    select distinct
        network_name
    from `voodoo-473511`.`USER_ACCQUISITION`.`stg_campaign_metrics`
)

select
    row_number() over() as network_id,
    network_name
from networks;

