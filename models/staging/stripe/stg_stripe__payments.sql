with src as (
    select *
    from {{ source('stripe', 'stripe_payments')}}
),

transformed as (

    select
        id as payment_id,
        orderid as order_id,
        paymentmethod as payment_method,
        status as payment_status,
        created as payment_created_at,
        {{ cents_to_dollars('amount') }} as payment_amount
    from src

)

select *
from transformed
