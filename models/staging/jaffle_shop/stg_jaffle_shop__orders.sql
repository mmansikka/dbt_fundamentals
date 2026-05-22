with src as (
    select *
    from {{ source('jaffle_shop', 'jaffle_shop_orders') }}
),

transformed as (

    select
        id as order_id,
        user_id as customer_id,
        order_date as order_placed_at,
        status as order_status,
        case 
            when status not in ('returned','return_pending') 
            then order_date 
        end as valid_order_date
    from src
)

select *
from transformed
