-- models/dim_date.sql
with date_range as (
    select 
        generate_series(
            '2000-01-01'::date, -- Start date
            current_date,       -- End date
            '1 day'::interval   -- Interval (daily)
        )::date as date_id
)

select 
    date_id,
    extract(year from date_id) as year,
    extract(quarter from date_id) as quarter,
    extract(month from date_id) as month,
    extract(day from date_id) as day,
    extract(week from date_id) as week,
    to_char(date_id, 'Day') as day_of_week,
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at
from date_range
