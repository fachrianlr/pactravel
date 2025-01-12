with dim_customers as (
    select 
        {{ dbt_utils.generate_surrogate_key( ["customer_id"] ) }} as sk_customer_id,
        customer_id as nk_customer_id,
        customer_first_name,
        customer_family_name,
        customer_gender,
        customer_birth_date,
        customer_country,
        customer_phone_number,
        {{ dbt_date.now() }} as created_at,
        {{ dbt_date.now() }} as updated_at
    from {{source('pactravel_dwh', 'customers')}}
)

select * from dim_customers