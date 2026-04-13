{{
    config(
        materialized='table',
        unique_key='quality_check_id'
    )
}}

with data_quality_checks as (
    -- Check 1: Campaign data completeness
    select 
        'campaign_data_completeness' as check_type,
        'fact_campaign_performance' as table_name,
        count(*) as total_records,
        sum(case when campaign_id is null then 1 else 0 end) as null_campaign_ids,
        sum(case when network_id is null then 1 else 0 end) as null_network_ids,
        sum(case when platform_id is null then 1 else 0 end) as null_platform_ids,
        sum(case when total_revenue < 0 then 1 else 0 end) as negative_revenue,
        sum(case when total_spend < 0 then 1 else 0 end) as negative_spend,
        sum(case when roas < 0 then 1 else 0 end) as negative_roas
    from {{ ref('fact_campaign_performance') }}
    
    union all
    
    -- Check 2: Spend data consistency
    select 
        'spend_data_consistency' as check_type,
        'stg_spend' as table_name,
        count(*) as total_records,
        sum(case when spend_id is null then 1 else 0 end) as null_spend_ids,
        sum(case when date is null then 1 else 0 end) as null_dates,
        sum(case when spend < 0 then 1 else 0 end) as negative_spend,
        sum(case when platform not in ('android', 'ios') then 1 else 0 end) as invalid_platforms,
        0 as null_network_ids,
        0 as null_platform_ids
    from {{ ref('stg_spend') }}
    
    union all
    
    -- Check 3: User data quality
    select 
        'user_data_quality' as check_type,
        'stg_user_data' as table_name,
        count(*) as total_records,
        sum(case when user_id is null then 1 else 0 end) as null_user_ids,
        sum(case when adid is null then 1 else 0 end) as null_adids,
        sum(case when install_date is null then 1 else 0 end) as null_install_dates,
        sum(case when platform not in ('android', 'ios') then 1 else 0 end) as invalid_platforms,
        sum(case when tracking_status not in ('Opt in', 'Opt out') then 1 else 0 end) as invalid_tracking_status,
        0 as null_platform_ids
    from {{ ref('stg_user_data') }}
    
    union all
    
    -- Check 4: Referential integrity
    select 
        'referential_integrity' as check_type,
        'foreign_key_constraints' as table_name,
        count(*) as total_records,
        sum(case when c.campaign_id is null then 1 else 0 end) as orphaned_campaigns,
        sum(case when n.network_id is null then 1 else 0 end) as orphaned_networks,
        sum(case when p.platform_id is null then 1 else 0 end) as orphaned_platforms,
        0 as null_network_ids,
        0 as null_platform_ids,
        0 as negative_revenue
    from {{ ref('fact_campaign_performance') }} f
    left join {{ ref('dim_campaign') }} c on f.campaign_id = c.campaign_id
    left join {{ ref('dim_network') }} n on f.network_id = n.network_id
    left join {{ ref('dim_platform') }} p on f.platform_id = p.platform_id
)

select
    {{ dbt_utils.generate_surrogate_key(['check_type', 'table_name']) }} as quality_check_id,
    check_type,
    table_name,
    total_records,
    null_campaign_ids,
    null_network_ids,
    null_platform_ids,
    null_spend_ids,
    null_dates,
    null_user_ids,
    null_adids,
    null_install_dates,
    negative_revenue,
    negative_spend,
    negative_roas,
    invalid_platforms,
    invalid_tracking_status,
    orphaned_campaigns,
    orphaned_networks,
    orphaned_platforms,
    
    -- Data quality score (0-100)
    case 
        when check_type = 'campaign_data_completeness' then
            round(
                (1.0 - (
                    (null_campaign_ids + null_network_ids + null_platform_ids + negative_revenue + negative_spend + negative_roas) / 
                    nullif(total_records, 0)
                )) * 100, 2
            )
        when check_type = 'spend_data_consistency' then
            round(
                (1.0 - (
                    (null_spend_ids + null_dates + negative_spend + invalid_platforms) / 
                    nullif(total_records, 0)
                )) * 100, 2
            )
        when check_type = 'user_data_quality' then
            round(
                (1.0 - (
                    (null_user_ids + null_adids + null_install_dates + invalid_platforms + invalid_tracking_status) / 
                    nullif(total_records, 0)
                )) * 100, 2
            )
        when check_type = 'referential_integrity' then
            round(
                (1.0 - (
                    (orphaned_campaigns + orphaned_networks + orphaned_platforms) / 
                    nullif(total_records, 0)
                )) * 100, 2
            )
        else 0
    end as data_quality_score,
    
    case 
        when data_quality_score >= 95 then 'Excellent'
        when data_quality_score >= 85 then 'Good'
        when data_quality_score >= 70 then 'Fair'
        else 'Poor'
    end as data_quality_status,
    
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
    
from data_quality_checks