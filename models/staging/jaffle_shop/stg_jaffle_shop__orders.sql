with src as (
    select *
    from {{ source('jaffle_shop', 'jaffle_shop_orders') }}
),

transformed as (

    select
        id as order_id,
        user_id as customer_id,
        order_date as order_placed_at,
        status as order_status
    from src
)

select *
from transformed
