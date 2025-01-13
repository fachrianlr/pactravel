-- models/fact_daily_bookings.sql

with flight_aggregates as (
    select 
        d.date_id,
        count(fb.trip_id) as total_flight_bookings,
        round(avg(fb.price), 2) as avg_flight_price
    from {{ ref('fact_flight_bookings') }} fb
    left join {{ ref('dim_date') }} d on fb.departure_date = d.date_id
    group by d.date_id
),

hotel_aggregates as (
    select 
        d.date_id,
        count(hb.trip_id) as total_hotel_bookings,
        round(avg(hb.price), 2) as avg_hotel_price
    from {{ ref('fact_hotel_bookings') }} hb
    left join {{ ref('dim_date') }} d on hb.check_in_date = d.date_id
    group by d.date_id
),

fact_daily_booking as (
    select 
        coalesce(fa.date_id, ha.date_id) as fact_date,
        coalesce(fa.total_flight_bookings, 0) as total_flight_bookings,
        coalesce(ha.total_hotel_bookings, 0) as total_hotel_bookings,
        coalesce(fa.avg_flight_price, 0.00) as avg_flight_price,
        coalesce(ha.avg_hotel_price, 0.00) as avg_hotel_price
    from flight_aggregates fa
    full outer join hotel_aggregates ha on fa.date_id = ha.date_id
)

select * from fact_daily_booking
