-- models/dim_aircraft.sql
with dim_aircrafts as (
    select 
        {{ dbt_utils.generate_surrogate_key( ["aircraft_id"] ) }} as sk_aircraft_id,
        aircraft_id as nk_aircraft_id,
        aircraft_name,
        aircraft_iata,
        aircraft_icao,
        {{ dbt_date.now() }} as created_at,
        {{ dbt_date.now() }} as updated_at
    from {{ source('pactravel_dwh', 'aircrafts') }}
)

select * from dim_aircrafts
