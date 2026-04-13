{{
    config(
        materialized='table',
        unique_key='date_id'
    )
}}

with date_spine as (
    {{ dbt_utils.date_spine(
        start_date="to_date('2024-01-01')",
        end_date="dateadd(year, 2, current_date())",
        datepart="day"
    ) }}
)

select
    {{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_id,
    date_day as date,
    year(date_day) as year,
    month(date_day) as month,
    day(date_day) as day,
    extract(quarter from date_day) as quarter,
    extract(week from date_day) as week_of_year,
    dayname(date_day) as day_of_week,
    case 
        when dayname(date_day) in ('Saturday', 'Sunday') then 'Weekend'
        else 'Weekday'
    end as day_type,
    case 
        when month(date_day) in (11, 12, 1) then 'Winter'
        when month(date_day) in (2, 3, 4) then 'Spring'
        when month(date_day) in (5, 6, 7) then 'Summer'
        when month(date_day) in (8, 9, 10) then 'Fall'
    end as season,
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
from date_spine