{% snapshot dim_customer_snapshot %}

{{
    config(
        target_database="pactravel-dwh",
        target_schema="snapshots",     
        unique_key="sk_customer_id",
        strategy="check",         
        check_cols=["customer_country"]
    )
}}

select 
    *
from {{ ref("dim_customers") }}

{% endsnapshot %}
