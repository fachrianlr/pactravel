-- models/fact_flight_bookings.sql
with fact_flight_bookings as (
    select 
        fb.trip_id,
        fb.customer_id,
        fb.flight_number,
        fb.airline_id,
        fb.aircraft_id,
        fb.airport_src,
        fb.airport_dst,
        fb.departure_time,
        fb.departure_date,
        fb.flight_duration,
        fb.travel_class,
        fb.seat_number,
        fb.price,
        d.date_id,
        ca.sk_customer_id,
        ai.sk_airline_id,
        ac.sk_aircraft_id,
        asrc.sk_airport_id as sk_airport_src,
        adst.sk_airport_id as sk_airport_dst
    from {{ source('pactravel_dwh', 'flight_bookings') }} fb
    left join {{ ref('dim_customers') }} ca on fb.customer_id = ca.nk_customer_id
    left join {{ ref('dim_airlines') }} ai on fb.airline_id = ai.nk_airline_id
    left join {{ ref('dim_aircraft') }} ac on fb.aircraft_id = ac.nk_aircraft_id
    left join {{ ref('dim_airports') }} asrc on fb.airport_src = asrc.nk_airport_id
    left join {{ ref('dim_airports') }} adst on fb.airport_dst = adst.nk_airport_id
    left join {{ ref('dim_date') }} d on CAST(fb.departure_date AS DATE) = d.date_id

)

select * from fact_flight_bookings
