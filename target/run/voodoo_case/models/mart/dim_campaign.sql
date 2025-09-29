

  create or replace view `voodoo-473511`.`USER_ACCQUISITION`.`dim_campaign`
  OPTIONS()
  as with campaigns as (
    select distinct
        campaign_name
    from `voodoo-473511`.`USER_ACCQUISITION`.`stg_campaign_metrics`
)

select
    row_number() over() as campaign_id,
    campaign_name
from campaigns;

