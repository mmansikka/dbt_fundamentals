with src as (
    select *
    from {{ source('jaffle_shop', 'jaffle_shop_customers')}}
),

transformed as (

    select
        id as customer_id,
        first_name as customer_first_name,
        last_name as customer_last_name,
        first_name || ' ' || last_name as full_name
    from src
)

select *
from transformed
