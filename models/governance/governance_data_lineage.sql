{{
    config(
        materialized='table',
        unique_key='lineage_id'
    )
}}

with model_lineage as (
    -- Define the data lineage for all models
    select 'spend' as source_table, 'stg_spend' as model_name, 'Staging' as layer, 'Raw spend data from advertising platforms' as description
    union all select 'user_data', 'stg_user_data', 'Staging', 'Raw user acquisition data'
    union all select 'dim_cv_bucket', 'stg_dim_cv_bucket', 'Staging', 'Conversion value bucket mapping'
    union all select 'campaign_analysis', 'stg_campaign_metrics', 'Staging', 'Campaign metrics from ML analysis'
    union all select null, 'dim_campaign', 'Mart', 'Campaign dimension table'
    union all select null, 'dim_network', 'Mart', 'Network dimension table'
    union all select null, 'dim_platform', 'Mart', 'Platform dimension table'
    union all select null, 'dim_date', 'Mart', 'Date dimension table'
    union all select null, 'dim_campaign_performance_tier', 'Mart', 'Campaign performance tier classification'
    union all select null, 'fact_campaign_performance', 'Mart', 'Campaign performance fact table'
    union all select null, 'fact_campaign_daily', 'Mart', 'Daily campaign performance metrics'
    union all select null, 'fact_campaign_summary', 'Mart', 'Campaign performance summary'
    union all select null, 'governance_data_quality', 'Governance', 'Data quality monitoring'
    union all select null, 'governance_data_lineage', 'Governance', 'Data lineage documentation'
),

table_stats as (
    -- Get table statistics from information_schema (this would need to be adapted for your data warehouse)
    select 
        'fact_campaign_performance' as table_name,
        count(*) as row_count,
        '2024-01-01' as last_updated  -- Replace with actual last updated timestamp
    from {{ ref('fact_campaign_performance') }}
    union all
    select 
        'fact_campaign_daily' as table_name,
        count(*) as row_count,
        '2024-01-01' as last_updated
    from {{ ref('fact_campaign_daily') }}
    union all
    select 
        'stg_spend' as table_name,
        count(*) as row_count,
        '2024-01-01' as last_updated
    from {{ ref('stg_spend') }}
    union all
    select 
        'stg_user_data' as table_name,
        count(*) as row_count,
        '2024-01-01' as last_updated
    from {{ ref('stg_user_data') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['l.model_name']) }} as lineage_id,
    l.source_table,
    l.model_name,
    l.layer,
    l.description,
    
    -- Table statistics
    coalesce(s.row_count, 0) as row_count,
    s.last_updated,
    
    -- Data ownership and classification
    case 
        when l.layer = 'Staging' then 'Data Engineering'
        when l.layer = 'Mart' then 'Analytics Engineering'
        when l.layer = 'Governance' then 'Data Governance'
        else 'Unknown'
    end as data_owner,
    
    case 
        when l.model_name like '%user%' or l.model_name like '%spend%' then 'PII/Financial'
        when l.model_name like '%campaign%' then 'Business Critical'
        else 'Standard'
    end as data_classification,
    
    -- Retention policy
    case 
        when l.layer = 'Staging' then '90 days'
        when l.layer = 'Mart' then '3 years'
        when l.layer = 'Governance' then '1 year'
        else 'N/A'
    end as retention_policy,
    
    -- Refresh frequency
    case 
        when l.layer = 'Staging' then 'Daily'
        when l.layer = 'Mart' then 'Daily'
        when l.layer = 'Governance' then 'Daily'
        else 'N/A'
    end as refresh_frequency,
    
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
    
from model_lineage l
left join table_stats s on l.model_name = s.table_name