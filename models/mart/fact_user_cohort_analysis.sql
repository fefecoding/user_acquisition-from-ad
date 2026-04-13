{{
    config(
        materialized='table',
        unique_key='cohort_analysis_id'
    )
}}

with user_cohorts as (
    select 
        adid,
        platform,
        install_date,
        date_trunc('month', install_date) as cohort_month,
        revenue_usd,
        case 
            when platform = 'ios' and tracking_status = 'Opt out' 
            then cv_bucket * 0.05  -- Estimated revenue for opt-out users
            else coalesce(revenue_usd, 0)
        end as adjusted_revenue
    from {{ ref('stg_user_data') }}
),

cohort_metrics as (
    select 
        cohort_month,
        platform,
        count(distinct adid) as cohort_size,
        sum(adjusted_revenue) as total_cohort_revenue,
        avg(adjusted_revenue) as avg_revenue_per_user,
        min(install_date) as cohort_start_date,
        max(install_date) as cohort_end_date
    from user_cohorts
    group by cohort_month, platform
),

retention_analysis as (
    select 
        uc.cohort_month,
        uc.platform,
        date_trunc('month', uc.install_date) as activity_month,
        datediff('month', uc.cohort_month, uc.activity_month) as months_since_install,
        count(distinct uc.adid) as active_users,
        sum(uc.adjusted_revenue) as monthly_revenue
    from user_cohorts uc
    group by uc.cohort_month, uc.platform, date_trunc('month', uc.install_date)
),

ltv_calculation as (
    select 
        cm.cohort_month,
        cm.platform,
        cm.cohort_size,
        cm.total_cohort_revenue,
        cm.avg_revenue_per_user as initial_arpu,
        
        -- LTV projections (3, 6, 12 months)
        cm.total_cohort_revenue / cm.cohort_size as ltv_current,
        (cm.total_cohort_revenue / cm.cohort_size) * 1.5 as ltv_3m_projected,
        (cm.total_cohort_revenue / cm.cohort_size) * 2.0 as ltv_6m_projected,
        (cm.total_cohort_revenue / cm.cohort_size) * 2.5 as ltv_12m_projected,
        
        -- Retention rates
        case 
            when ra.active_users > 0 
            then round(ra.active_users::float / cm.cohort_size::float * 100, 2)
            else 0 
        end as retention_rate,
        
        cm.cohort_start_date,
        cm.cohort_end_date
    from cohort_metrics cm
    left join retention_analysis ra 
        on cm.cohort_month = ra.cohort_month 
        and cm.platform = ra.platform
        and ra.months_since_install = (
            select max(months_since_install) 
            from retention_analysis ra2 
            where ra2.cohort_month = cm.cohort_month 
            and ra2.platform = cm.platform
        )
)

select
    {{ dbt_utils.generate_surrogate_key(['cohort_month', 'platform']) }} as cohort_analysis_id,
    cohort_month,
    platform,
    cohort_size,
    total_cohort_revenue,
    initial_arpu,
    ltv_current,
    ltv_3m_projected,
    ltv_6m_projected,
    ltv_12m_projected,
    retention_rate,
    cohort_start_date,
    cohort_end_date,
    
    -- Cohort quality score (0-100)
    (
        case when ltv_current > 5.0 then 40 else 0 end +
        case when retention_rate > 30 then 30 else 0 end +
        case when total_cohort_revenue > 10000 then 20 else 0 end +
        case when cohort_size > 1000 then 10 else 0 end
    ) as cohort_quality_score,
    
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
    
from ltv_calculation
order by cohort_month desc, platform