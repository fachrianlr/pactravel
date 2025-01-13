-- models/fact_hotel_bookings.sql
with fact_hotel_bookings as (
    select 
        hb.trip_id,
        hb.customer_id,
        hb.hotel_id,
        CAST(hb.check_in_date AS DATE),
        CAST(hb.check_out_date AS DATE),
        hb.price,
        hb.breakfast_included,
        ca.sk_customer_id,
        ho.sk_hotel_id
    from {{ source('pactravel_dwh', 'hotel_bookings') }} hb
    left join {{ ref('dim_customers') }} ca on hb.customer_id = ca.nk_customer_id
    left join {{ ref('dim_hotels') }} ho on hb.hotel_id = ho.nk_hotel_id
)

select * from fact_hotel_bookings
