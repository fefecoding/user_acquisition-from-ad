{{
    config(
        materialized='table',
        unique_key='attribution_id'
    )
}}

with user_journey as (
    -- Build user journey with all touchpoints
    select 
        adid,
        platform,
        install_date,
        campaign_name,
        network_name,
        revenue_usd,
        case 
            when platform = 'ios' and tracking_status = 'Opt out' 
            then cv_bucket * 0.05
            else coalesce(revenue_usd, 0)
        end as adjusted_revenue,
        row_number() over (
            partition by adid 
            order by install_date asc, campaign_name asc
        ) as touchpoint_order
    from {{ ref('stg_user_data') }}
    where campaign_name is not null and network_name is not null
),

campaign_spend as (
    select 
        platform,
        campaign_name,
        network_name,
        sum(spend) as total_spend
    from {{ ref('stg_spend') }}
    group by platform, campaign_name, network_name
),

-- First-touch attribution (credits first interaction)
first_touch as (
    select 
        platform,
        campaign_name,
        network_name,
        count(distinct adid) as first_touch_users,
        sum(adjusted_revenue) as first_touch_revenue
    from user_journey
    where touchpoint_order = 1
    group by platform, campaign_name, network_name
),

-- Last-touch attribution (credits last interaction)
last_touch as (
    select 
        platform,
        campaign_name,
        network_name,
        count(distinct adid) as last_touch_users,
        sum(adjusted_revenue) as last_touch_revenue
    from user_journey
    where touchpoint_order = (
            select max(touchpoint_order) 
            from user_journey uj2 
            where uj2.adid = user_journey.adid
        )
    group by platform, campaign_name, network_name
),

-- Linear attribution (equal credit to all touchpoints)
linear_attribution as (
    select 
        platform,
        campaign_name,
        network_name,
        count(distinct adid) as linear_users,
        sum(adjusted_revenue / touchpoint_order) as linear_revenue
    from user_journey
    group by platform, campaign_name, network_name
),

-- Time-decay attribution (more credit to recent touchpoints)
time_decay as (
    select 
        platform,
        campaign_name,
        network_name,
        count(distinct adid) as time_decay_users,
        sum(adjusted_revenue * (touchpoint_order::float / (
            select max(touchpoint_order) 
            from user_journey uj2 
            where uj2.adid = user_journey.adid
        ))) as time_decay_revenue
    from user_journey
    group by platform, campaign_name, network_name
),

-- Position-based attribution (40% first, 40% last, 20% middle)
position_based as (
    select 
        platform,
        campaign_name,
        network_name,
        count(distinct adid) as position_users,
        sum(
            case 
                when touchpoint_order = 1 then adjusted_revenue * 0.4
                when touchpoint_order = (
                    select max(touchpoint_order) 
                    from user_journey uj2 
                    where uj2.adid = user_journey.adid
                ) then adjusted_revenue * 0.4
                else adjusted_revenue * 0.2
            end
        ) as position_revenue
    from user_journey
    group by platform, campaign_name, network_name
)

select
    {{ dbt_utils.generate_surrogate_key([
        'coalesce(ft.campaign_name, lt.campaign_name, la.campaign_name, td.campaign_name, pb.campaign_name)',
        'coalesce(ft.network_name, lt.network_name, la.network_name, td.network_name, pb.network_name)',
        'coalesce(ft.platform, lt.platform, la.platform, td.platform, pb.platform)'
    ]) }} as attribution_id,
    coalesce(ft.platform, lt.platform, la.platform, td.platform, pb.platform) as platform,
    coalesce(ft.campaign_name, lt.campaign_name, la.campaign_name, td.campaign_name, pb.campaign_name) as campaign_name,
    coalesce(ft.network_name, lt.network_name, la.network_name, td.network_name, pb.network_name) as network_name,
    
    -- First-touch metrics
    coalesce(ft.first_touch_users, 0) as first_touch_users,
    coalesce(ft.first_touch_revenue, 0) as first_touch_revenue,
    case 
        when cs.total_spend > 0 
        then ft.first_touch_revenue / cs.total_spend 
        else 0 
    end as first_touch_roas,
    
    -- Last-touch metrics
    coalesce(lt.last_touch_users, 0) as last_touch_users,
    coalesce(lt.last_touch_revenue, 0) as last_touch_revenue,
    case 
        when cs.total_spend > 0 
        then lt.last_touch_revenue / cs.total_spend 
        else 0 
    end as last_touch_roas,
    
    -- Linear attribution metrics
    coalesce(la.linear_users, 0) as linear_users,
    coalesce(la.linear_revenue, 0) as linear_revenue,
    case 
        when cs.total_spend > 0 
        then la.linear_revenue / cs.total_spend 
        else 0 
    end as linear_roas,
    
    -- Time-decay attribution metrics
    coalesce(td.time_decay_users, 0) as time_decay_users,
    coalesce(td.time_decay_revenue, 0) as time_decay_revenue,
    case 
        when cs.total_spend > 0 
        then td.time_decay_revenue / cs.total_spend 
        else 0 
    end as time_decay_roas,
    
    -- Position-based attribution metrics
    coalesce(pb.position_users, 0) as position_users,
    coalesce(pb.position_revenue, 0) as position_revenue,
    case 
        when cs.total_spend > 0 
        then pb.position_revenue / cs.total_spend 
        else 0 
    end as position_roas,
    
    -- Recommended attribution model based on data
    case 
        when (ft.first_touch_revenue + lt.last_touch_revenue) > (la.linear_revenue * 2) 
        then 'Position-based (assisted conversions important)'
        when la.linear_revenue > lt.last_touch_revenue * 1.5 
        then 'Linear (multi-touch journey common)'
        when td.time_decay_revenue > lt.last_touch_revenue * 1.2 
        then 'Time-decay (recent interactions matter)'
        else 'Last-touch (direct response dominant)'
    end as recommended_model,
    
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
    
from coalesce(ft, lt, la, td, pb)
left join campaign_spend cs 
    on coalesce(ft.platform, lt.platform, la.platform, td.platform, pb.platform) = cs.platform
    and coalesce(ft.campaign_name, lt.campaign_name, la.campaign_name, td.campaign_name, pb.campaign_name) = cs.campaign_name
    and coalesce(ft.network_name, lt.network_name, la.network_name, td.network_name, pb.network_name) = cs.network_name