{{
    config(
        materialized='incremental',
        unique_key='user_id',
        incremental_strategy='merge'
    )
}}

with source as (
    select * from {{ ref('user_data') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'adid', 
            'install_date', 
            'platform'
        ]) }} as user_id,
        adid,
        platform,
        install_date,
        campaign_name,
        network_name,
        revenue_usd,
        cv_bucket,
        tracking_status,
        {{ dbt_date.now() }} as created_at,
        {{ dbt_date.now() }} as updated_at
    from source
    {% if is_incremental() %}
        where install_date > (select max(install_date) from {{ this }})
    {% endif %}
)

select * from final