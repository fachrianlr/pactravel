-- models/dim_hotels.sql
with dim_hotels as (
    select 
        {{ dbt_utils.generate_surrogate_key( ["hotel_id"] ) }} as sk_hotel_id,
        hotel_id as nk_hotel_id,
        hotel_name,
        hotel_address,
        city,
        country,
        hotel_score,
        {{ dbt_date.now() }} as created_at,
        {{ dbt_date.now() }} as updated_at
    from {{ source('pactravel_dwh', 'hotel') }}
)

select * from dim_hotels
