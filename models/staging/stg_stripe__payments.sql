{{
    config(
        materialized = 'view'
    )
}}

with src as (
    select
        * except (amount),
        {{ cents_to_dollars('amount') }} as amount
    from {{ source('stripe', 'stripe_payments')}}
)

select *
from src
