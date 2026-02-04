{{
    config(
        materialized = 'view'
    )
}}

with src as (
    select
        id as payment_id,
        orderid as order_id,
        paymentmethod as payment_method,
        status,
        created as payment_date,
        {{ cents_to_dollars('amount') }} as amount
    from {{ source('stripe', 'stripe_payments')}}

)

select *
from src
