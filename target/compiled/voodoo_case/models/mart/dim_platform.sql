with platforms as (
    select distinct
        platform
    from `voodoo-473511`.`USER_ACCQUISITION`.`stg_campaign_metrics`
)

select
    row_number() over() as platform_id,
    platform
from platforms