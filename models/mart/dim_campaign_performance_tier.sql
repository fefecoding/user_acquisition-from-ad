{{
    config(
        materialized='table',
        unique_key='performance_tier_id'
    )
}}

with campaign_performance as (
    select * from {{ ref('fact_campaign_performance') }}
),

ranked_campaigns as (
    select
        campaign_id,
        roas,
        arpu,
        total_revenue,
        total_users,
        high_roas_prob,
        conversion_rate,
        
        -- ROAS-based tier
        case 
            when roas >= 2.0 then 'Tier 1 - Excellent'
            when roas >= 1.0 then 'Tier 2 - Good'
            when roas >= 0.5 then 'Tier 3 - Average'
            else 'Tier 4 - Poor'
        end as roas_tier,
        
        -- Revenue-based tier
        case 
            when total_revenue >= 100000 then 'High Revenue'
            when total_revenue >= 50000 then 'Medium Revenue'
            when total_revenue >= 10000 then 'Low Revenue'
            else 'Very Low Revenue'
        end as revenue_tier,
        
        -- Overall performance score
        (
            (roas * 0.4) + 
            (arpu * 0.2) + 
            (high_roas_prob * 0.2) + 
            (case when conversion_rate > 50 then 0.2 else 0 end)
        ) as performance_score
        
    from campaign_performance
)

select
    {{ dbt_utils.generate_surrogate_key(['campaign_id', 'roas_tier', 'revenue_tier']) }} as performance_tier_id,
    campaign_id,
    roas,
    arpu,
    total_revenue,
    total_users,
    high_roas_prob,
    conversion_rate,
    roas_tier,
    revenue_tier,
    performance_score,
    
    -- Recommended actions
    case 
        when roas >= 2.0 and total_revenue >= 100000 then 'Scale up - Increase budget'
        when roas >= 1.0 and roas < 2.0 then 'Optimize - Maintain budget, improve targeting'
        when roas >= 0.5 and roas < 1.0 then 'Review - Reduce budget, analyze performance'
        else 'Pause - Poor performance, reallocate budget'
    end as recommended_action,
    
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
    
from ranked_campaigns