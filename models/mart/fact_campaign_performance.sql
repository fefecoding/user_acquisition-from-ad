{{
    config(
        materialized='table',
        unique_key='campaign_performance_id'
    )
}}

with campaign_metrics as (
    select * from {{ ref('stg_campaign_metrics') }}
),

dim_campaign as (
    select * from {{ ref('dim_campaign') }}
),

dim_network as (
    select * from {{ ref('dim_network') }}
),

dim_platform as (
    select * from {{ ref('dim_platform') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
),

-- Get the current date for performance tracking
current_date_dim as (
    select date_id 
    from dim_date 
    where date = current_date()
)

select
    {{ dbt_utils.generate_surrogate_key([
        'c.campaign_id', 
        'n.network_id', 
        'p.platform_id',
        'm.total_revenue',
        'm.total_users'
    ]) }} as campaign_performance_id,
    c.campaign_id,
    n.network_id,
    p.platform_id,
    coalesce(cd.date_id, (select date_id from current_date_dim)) as date_id,
    m.total_revenue,
    m.total_users,
    m.total_spend,
    m.arpu,
    m.roas,
    m.conversion_rate,
    m.forecasted_revenue,
    m.high_roas_prob,
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
from campaign_metrics m
inner join dim_campaign c 
    on m.campaign_name = c.campaign_name
inner join dim_network n 
    on m.network_name = n.network_name
inner join dim_platform p 
    on m.platform = p.platform
cross join current_date_dim cd