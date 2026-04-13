{{
    config(
        materialized='table',
        unique_key='seasonality_id'
    )
}}

with daily_performance as (
    select 
        dd.date,
        dd.year,
        dd.month,
        dd.quarter,
        dd.day_of_week,
        dd.season,
        dd.week_of_year,
        p.platform,
        n.network_name,
        c.campaign_name,
        cd.daily_revenue,
        cd.daily_spend,
        cd.daily_roas,
        cd.daily_arpu,
        cd.daily_users
    from {{ ref('fact_campaign_daily') }} cd
    join {{ ref('dim_date') }} dd on cd.date_id = dd.date_id
    join {{ ref('dim_platform') }} p on cd.platform_id = p.platform_id
    join {{ ref('dim_network') }} n on cd.network_id = n.network_id
    join {{ ref('dim_campaign') }} c on cd.campaign_id = c.campaign_id
),

-- Seasonal patterns analysis
seasonal_patterns as (
    select 
        platform,
        network_name,
        campaign_name,
        season,
        count(distinct date) as days_in_season,
        sum(daily_revenue) as seasonal_revenue,
        sum(daily_spend) as seasonal_spend,
        avg(daily_roas) as avg_seasonal_roas,
        avg(daily_arpu) as avg_seasonal_arpu,
        sum(daily_users) as seasonal_users,
        
        -- Seasonal performance index (100 = average)
        case 
            when avg(daily_roas) > 0 then 
                round(avg(daily_roas) / (
                    select avg(daily_roas) 
                    from daily_performance dp2 
                    where dp2.platform = daily_performance.platform
                ) * 100, 2)
            else 100 
        end as seasonal_performance_index
    from daily_performance
    group by platform, network_name, campaign_name, season
),

-- Day of week patterns
day_of_week_patterns as (
    select 
        platform,
        day_of_week,
        count(distinct date) as occurrences,
        avg(daily_revenue) as avg_daily_revenue,
        avg(daily_roas) as avg_daily_roas,
        avg(daily_arpu) as avg_daily_arpu,
        sum(daily_users) as total_users,
        
        -- Day of week performance index
        case 
            when avg(daily_roas) > 0 then 
                round(avg(daily_roas) / (
                    select avg(daily_roas) 
                    from daily_performance dp2 
                    where dp2.platform = daily_performance.platform
                ) * 100, 2)
            else 100 
        end as dow_performance_index
    from daily_performance
    group by platform, day_of_week
),

-- Monthly trends
monthly_trends as (
    select 
        platform,
        month,
        count(distinct date) as days_in_month,
        sum(daily_revenue) as monthly_revenue,
        sum(daily_spend) as monthly_spend,
        avg(daily_roas) as avg_monthly_roas,
        avg(daily_arpu) as avg_monthly_arpu,
        sum(daily_users) as monthly_users,
        
        -- Month-over-month growth
        lag(sum(daily_revenue)) over (
            partition by platform 
            order by month
        ) as prev_month_revenue,
        
        case 
            when lag(sum(daily_revenue)) over (
                partition by platform 
                order by month
            ) > 0 then
                round(
                    (sum(daily_revenue) - lag(sum(daily_revenue)) over (
                        partition by platform 
                        order by month
                    )) / lag(sum(daily_revenue)) over (
                        partition by platform 
                        order by month
                    ) * 100, 2
                )
            else 0 
        end as mom_growth_percent
    from daily_performance
    group by platform, month
),

-- Week number patterns (for weekly seasonality)
weekly_patterns as (
    select 
        platform,
        week_of_year,
        count(distinct date) as weeks_tracked,
        avg(daily_revenue) as avg_weekly_revenue,
        avg(daily_roas) as avg_weekly_roas,
        
        -- Week performance classification
        case 
            when avg(daily_roas) >= (
                select avg(daily_roas) * 1.2 
                from daily_performance dp2 
                where dp2.platform = daily_performance.platform
            ) then 'High Performance Week'
            when avg(daily_roas) <= (
                select avg(daily_roas) * 0.8 
                from daily_performance dp2 
                where dp2.platform = daily_performance.platform
            ) then 'Low Performance Week'
            else 'Average Week'
        end as week_performance_tier
    from daily_performance
    group by platform, week_of_year
),

-- Holiday/Special event detection (simplified)
special_periods as (
    select 
        platform,
        case 
            when month = 12 and day_of_week in ('Saturday', 'Sunday') then 'Holiday Season Weekend'
            when month = 12 then 'Holiday Season'
            when month = 11 and day_of_week = 'Friday' then 'Black Friday Period'
            when month = 1 and day_of_week in ('Saturday', 'Sunday') then 'New Year Weekend'
            when month in (6, 7, 8) and day_of_week in ('Saturday', 'Sunday') then 'Summer Weekend'
            else 'Regular Period'
        end as special_period,
        count(distinct date) as days_in_period,
        avg(daily_revenue) as avg_daily_revenue,
        avg(daily_roas) as avg_daily_roas,
        sum(daily_users) as total_users
    from daily_performance
    group by platform, 
        case 
            when month = 12 and day_of_week in ('Saturday', 'Sunday') then 'Holiday Season Weekend'
            when month = 12 then 'Holiday Season'
            when month = 11 and day_of_week = 'Friday' then 'Black Friday Period'
            when month = 1 and day_of_week in ('Saturday', 'Sunday') then 'New Year Weekend'
            when month in (6, 7, 8) and day_of_week in ('Saturday', 'Sunday') then 'Summer Weekend'
            else 'Regular Period'
        end
)

select
    {{ dbt_utils.generate_surrogate_key([
        'sp.platform', 
        'sp.network_name', 
        'sp.campaign_name', 
        'sp.season',
        'dow.day_of_week',
        'mt.month'
    ]) }} as seasonality_id,
    sp.platform,
    sp.network_name,
    sp.campaign_name,
    
    -- Seasonal metrics
    sp.season,
    sp.days_in_season,
    sp.seasonal_revenue,
    sp.seasonal_spend,
    sp.avg_seasonal_roas,
    sp.avg_seasonal_arpu,
    sp.seasonal_users,
    sp.seasonal_performance_index,
    
    -- Day of week metrics
    dow.day_of_week,
    dow.occurrences,
    dow.avg_daily_revenue as dow_avg_revenue,
    dow.avg_daily_roas as dow_avg_roas,
    dow.avg_daily_arpu as dow_avg_arpu,
    dow.total_users as dow_total_users,
    dow.dow_performance_index,
    
    -- Monthly metrics
    mt.month,
    mt.days_in_month,
    mt.monthly_revenue,
    mt.monthly_spend,
    mt.avg_monthly_roas,
    mt.avg_monthly_arpu,
    mt.monthly_users,
    mt.mom_growth_percent,
    
    -- Week patterns
    wp.week_of_year,
    wp.weeks_tracked,
    wp.avg_weekly_revenue,
    wp.avg_weekly_roas,
    wp.week_performance_tier,
    
    -- Special periods
    sp2.special_period,
    sp2.days_in_period,
    sp2.avg_daily_revenue as special_avg_revenue,
    sp2.avg_daily_roas as special_avg_roas,
    sp2.total_users as special_total_users,
    
    -- Seasonality insights
    case 
        when sp.seasonal_performance_index >= 120 then 'Peak Season'
        when sp.seasonal_performance_index >= 100 then 'Good Season'
        when sp.seasonal_performance_index >= 80 then 'Average Season'
        else 'Low Season'
    end as season_classification,
    
    case 
        when dow.dow_performance_index >= 120 then 'Best Day'
        when dow.dow_performance_index >= 100 then 'Good Day'
        when dow.dow_performance_index >= 80 then 'Average Day'
        else 'Poor Day'
    end as day_classification,
    
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
    
from seasonal_patterns sp
left join day_of_week_patterns dow on sp.platform = dow.platform
left join monthly_trends mt on sp.platform = mt.platform
left join weekly_patterns wp on sp.platform = wp.platform
left join special_periods sp2 on sp.platform = sp2.platform
order by sp.platform, sp.seasonal_performance_index desc