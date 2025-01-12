-- models/dim_airlines.sql
with dim_airlines as (
    select 
        {{ dbt_utils.generate_surrogate_key( ["airline_id"] ) }} as sk_airline_id,
        airline_id as nk_airline_id,
        airline_name,
        country,
        airline_iata,
        airline_icao,
        alias,
        {{ dbt_date.now() }} as created_at,
        {{ dbt_date.now() }} as updated_at
    from {{ source('pactravel_dwh', 'airlines') }}
)

select * from dim_airlines
