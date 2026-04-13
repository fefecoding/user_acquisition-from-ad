{{
    config(
        materialized='table',
        unique_key='summary_id'
    )
}}

with campaign_performance as (
    select * from {{ ref('fact_campaign_performance') }}
),

campaign_daily as (
    select * from {{ ref('fact_campaign_daily') }}
),

dim_campaign as (select * from {{ ref('dim_campaign') }}),
dim_network as (select * from {{ ref('dim_network') }}),
dim_platform as (select * from {{ ref('dim_platform') }})

select
    {{ dbt_utils.generate_surrogate_key([
        'cp.campaign_id', 
        'cp.network_id', 
        'cp.platform_id'
    ]) }} as summary_id,
    cp.campaign_id,
    cp.network_id,
    cp.platform_id,
    c.campaign_name,
    n.network_name,
    p.platform,
    
    -- Performance metrics
    cp.total_revenue,
    cp.total_users,
    cp.total_spend,
    cp.arpu,
    cp.roas,
    cp.conversion_rate,
    cp.forecasted_revenue,
    cp.high_roas_prob,
    
    -- Daily aggregated metrics (last 30 days)
    sum(cd.daily_revenue) as last_30d_revenue,
    sum(cd.daily_users) as last_30d_users,
    sum(cd.daily_spend) as last_30d_spend,
    avg(cd.daily_roas) as avg_30d_roas,
    avg(cd.daily_arpu) as avg_30d_arpu,
    
    -- Growth metrics
    case 
        when lag(cp.total_revenue) over (partition by cp.campaign_id order by cp.created_at) > 0
        then (cp.total_revenue - lag(cp.total_revenue) over (partition by cp.campaign_id order by cp.created_at)) 
             / lag(cp.total_revenue) over (partition by cp.campaign_id order by cp.created_at)
        else 0 
    end as revenue_growth_rate,
    
    -- Efficiency metrics
    case 
        when cp.total_spend > 0 
        then cp.total_revenue / cp.total_spend 
        else 0 
    end as overall_roas,
    
    -- Campaign health score (0-100)
    (
        case when cp.roas > 1.0 then 40 else 0 end +
        case when cp.conversion_rate > 50 then 30 else 0 end +
        case when cp.arpu > 2.0 then 20 else 0 end +
        case when cp.high_roas_prob > 0.7 then 10 else 0 end
    ) as campaign_health_score,
    
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
    
from campaign_performance cp
left join campaign_daily cd 
    on cp.campaign_id = cd.campaign_id
    and cp.network_id = cd.network_id
    and cp.platform_id = cd.platform_id
    and cd.date >= date_sub(current_date(), interval 30 day)
inner join dim_campaign c on cp.campaign_id = c.campaign_id
inner join dim_network n on cp.network_id = n.network_id
inner join dim_platform p on cp.platform_id = p.platform_id
group by 
    cp.campaign_id, cp.network_id, cp.platform_id,
    c.campaign_name, n.network_name, p.platform,
    cp.total_revenue, cp.total_users, cp.total_spend,
    cp.arpu, cp.roas, cp.conversion_rate, cp.forecasted_revenue, cp.high_roas_prob