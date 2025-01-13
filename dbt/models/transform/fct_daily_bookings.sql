-- models/fact_daily_bookings.sql

with flight_aggregates as (
    select 
        d.date_id,
        count(fb.trip_id) as total_flight_bookings,
        round(avg(fb.price), 2) as avg_flight_price
    from {{ source('pactravel_dwh', 'flight_bookings') }} fb
    left join {{ ref('dim_date') }} d on cast(fb.departure_date as date) = d.date_id
    group by d.date_id
),

hotel_aggregates as (
    select 
        d.date_id,
        count(hb.trip_id) as total_hotel_bookings,
        round(avg(hb.price), 2) as avg_hotel_price
    from {{ source('pactravel_dwh', 'hotel_bookings') }} hb
    left join {{ ref('dim_date') }} d on cast(hb.check_in_date as date) = d.date_id
    group by d.date_id
),

daily_booking_facts as (
    select 
        coalesce(fa.date_id, ha.date_id) as fact_date,
        coalesce(fa.total_flight_bookings, 0) as total_flight_bookings,
        coalesce(ha.total_hotel_bookings, 0) as total_hotel_bookings,
        coalesce(fa.avg_flight_price, 0.00) as avg_flight_price,
        coalesce(ha.avg_hotel_price, 0.00) as avg_hotel_price
    from flight_aggregates fa
    full outer join hotel_aggregates ha on fa.date_id = ha.date_id
)

select * from daily_booking_facts
