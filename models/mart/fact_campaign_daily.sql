{{
    config(
        materialized='incremental',
        unique_key='campaign_daily_id',
        incremental_strategy='merge',
        partition_by={
            "field": "date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=["platform_id", "network_id", "campaign_id"]
    )
}}

with spend_data as (
    select 
        date,
        platform,
        network_name,
        campaign_name,
        sum(spend) as daily_spend
    from {{ ref('stg_spend') }}
    {% if is_incremental() %}
        where date >= date_sub(current_date(), interval 7 day)
    {% endif %}
    group by date, platform, network_name, campaign_name
),

user_data as (
    select 
        install_date as date,
        platform,
        network_name,
        campaign_name,
        count(distinct adid) as daily_users,
        sum(case 
            when platform = 'ios' and tracking_status = 'Opt out' 
            then coalesce(cv_bucket * 0.05, 0)  -- Estimated revenue for opt-out users
            else coalesce(revenue_usd, 0)
        end) as daily_revenue
    from {{ ref('stg_user_data') }}
    {% if is_incremental() %}
        where install_date >= date_sub(current_date(), interval 7 day)
    {% endif %}
    group by date, platform, network_name, campaign_name
),

dim_campaign as (select * from {{ ref('dim_campaign') }}),
dim_network as (select * from {{ ref('dim_network') }}),
dim_platform as (select * from {{ ref('dim_platform') }}),
dim_date as (select * from {{ ref('dim_date') }})

select
    {{ dbt_utils.generate_surrogate_key([
        'd.date', 
        'c.campaign_id', 
        'n.network_id', 
        'p.platform_id'
    ]) }} as campaign_daily_id,
    c.campaign_id,
    n.network_id,
    p.platform_id,
    dd.date_id,
    d.date,
    d.daily_users,
    d.daily_revenue,
    s.daily_spend,
    case 
        when s.daily_spend > 0 
        then d.daily_revenue / s.daily_spend 
        else 0 
    end as daily_roas,
    case 
        when d.daily_users > 0 
        then d.daily_revenue / d.daily_users 
        else 0 
    end as daily_arpu,
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
from (
    select 
        coalesce(s.date, u.date) as date,
        coalesce(s.platform, u.platform) as platform,
        coalesce(s.network_name, u.network_name) as network_name,
        coalesce(s.campaign_name, u.campaign_name) as campaign_name,
        coalesce(u.daily_users, 0) as daily_users,
        coalesce(u.daily_revenue, 0) as daily_revenue,
        coalesce(s.daily_spend, 0) as daily_spend
    from spend_data s
    full outer join user_data u
        on s.date = u.date
        and s.platform = u.platform
        and s.network_name = u.network_name
        and s.campaign_name = u.campaign_name
) d
inner join dim_campaign c on d.campaign_name = c.campaign_name
inner join dim_network n on d.network_name = n.network_name
inner join dim_platform p on d.platform = p.platform
inner join dim_date dd on d.date = dd.date