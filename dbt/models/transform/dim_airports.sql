-- models/dim_airports.sql
with dim_airports as (
    select 
        {{ dbt_utils.generate_surrogate_key( ["airport_id"] ) }} as sk_airport_id,
        airport_id as nk_airport_id,
        airport_name,
        city,
        latitude,
        longitude,
        {{ dbt_date.now() }} as created_at,
        {{ dbt_date.now() }} as updated_at
    from {{ source('pactravel_dwh', 'airports') }}
)

select * from dim_airports
