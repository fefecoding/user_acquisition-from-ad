{{
    config(
        materialized='incremental',
        unique_key='spend_id',
        incremental_strategy='merge'
    )
}}

with source as (
    select * from {{ ref('spend') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'date', 
            'platform', 
            'network_name', 
            'campaign_name',
            'spend'
        ]) }} as spend_id,
        date,
        platform,
        network_name,
        campaign_name,
        spend,
        {{ dbt_date.now() }} as created_at,
        {{ dbt_date.now() }} as updated_at
    from source
    {% if is_incremental() %}
        where date > (select max(date) from {{ this }})
    {% endif %}
)

select * from final