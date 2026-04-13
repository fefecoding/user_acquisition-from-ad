{{
    config(
        materialized='table',
        unique_key='optimization_id'
    )
}}

with campaign_performance as (
    select 
        c.campaign_name,
        n.network_name,
        p.platform,
        cp.total_revenue,
        cp.total_spend,
        cp.roas,
        cp.arpu,
        cp.total_users,
        cp.conversion_rate,
        cp.high_roas_prob,
        cp.forecasted_revenue
    from {{ ref('fact_campaign_performance') }} cp
    join {{ ref('dim_campaign') }} c on cp.campaign_id = c.campaign_id
    join {{ ref('dim_network') }} n on cp.network_id = n.network_id
    join {{ ref('dim_platform') }} p on cp.platform_id = p.platform_id
),

spend_allocation as (
    select 
        campaign_name,
        network_name,
        platform,
        total_revenue,
        total_spend,
        roas,
        arpu,
        total_users,
        conversion_rate,
        high_roas_prob,
        forecasted_revenue,
        
        -- Current efficiency metrics
        case 
            when total_spend > 0 then total_revenue / total_spend 
            else 0 
        end as current_roas,
        
        -- Efficiency score (0-100)
        (
            (roas * 40) + 
            (high_roas_prob * 30) + 
            (conversion_rate / 100 * 20) + 
            (case when arpu > 2.0 then 10 else 0 end)
        ) as efficiency_score,
        
        -- Budget allocation recommendations
        case 
            when roas > 2.0 and high_roas_prob > 0.8 then 'Scale Up'
            when roas > 1.0 and roas <= 2.0 then 'Maintain'
            when roas > 0.5 and roas <= 1.0 then 'Optimize'
            else 'Reduce'
        end as budget_recommendation
    from campaign_performance
),

budget_simulation as (
    select 
        campaign_name,
        network_name,
        platform,
        total_spend,
        efficiency_score,
        budget_recommendation,
        
        -- Simulate budget changes
        case 
            when budget_recommendation = 'Scale Up' then total_spend * 1.5
            when budget_recommendation = 'Maintain' then total_spend * 1.0
            when budget_recommendation = 'Optimize' then total_spend * 0.8
            else total_spend * 0.5
        end as recommended_spend,
        
        case 
            when budget_recommendation = 'Scale Up' then total_spend * 0.5
            when budget_recommendation = 'Maintain' then total_spend * 0.0
            when budget_recommendation = 'Optimize' then total_spend * -0.2
            else total_spend * -0.5
        end as spend_change,
        
        -- Expected ROAS based on efficiency
        case 
            when efficiency_score >= 80 then 2.5
            when efficiency_score >= 60 then 1.8
            when efficiency_score >= 40 then 1.2
            else 0.8
        end as expected_roas
    from spend_allocation
),

optimization_summary as (
    select 
        campaign_name,
        network_name,
        platform,
        total_spend,
        efficiency_score,
        budget_recommendation,
        recommended_spend,
        spend_change,
        expected_roas,
        
        -- Projected outcomes
        recommended_spend * expected_roas as projected_revenue,
        (recommended_spend * expected_roas) - recommended_spend as projected_profit,
        
        -- ROI improvement
        case 
            when total_spend > 0 
            then ((recommended_spend * expected_roas) - total_spend) / total_spend * 100
            else 0 
        end as roi_improvement_percent
    from budget_simulation
)

select
    {{ dbt_utils.generate_surrogate_key(['campaign_name', 'network_name', 'platform']) }} as optimization_id,
    campaign_name,
    network_name,
    platform,
    total_spend,
    efficiency_score,
    budget_recommendation,
    recommended_spend,
    spend_change,
    expected_roas,
    projected_revenue,
    projected_profit,
    roi_improvement_percent,
    
    -- Budget allocation priority
    case 
        when efficiency_score >= 80 and budget_recommendation = 'Scale Up' then 'High Priority'
        when efficiency_score >= 60 and budget_recommendation = 'Maintain' then 'Medium Priority'
        when efficiency_score >= 40 and budget_recommendation = 'Optimize' then 'Low Priority'
        else 'Review Required'
    end as allocation_priority,
    
    -- Risk assessment
    case 
        when efficiency_score >= 80 then 'Low Risk'
        when efficiency_score >= 60 then 'Medium Risk'
        else 'High Risk'
    end as risk_level,
    
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
    
from optimization_summary
order by efficiency_score desc, roi_improvement_percent desc