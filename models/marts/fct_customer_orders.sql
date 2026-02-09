{{ config(materialized="view") }}

with

orders as (
    
    select *
    from {{ source("jaffle_shop", "jaffle_shop_orders") }}

),

customers as (
    
    select *
    from {{ source("jaffle_shop", "jaffle_shop_customers") }}

),

payments as (
    
    select *
    from {{ source("stripe", "stripe_payments") }}

),

aggregated_payments as (

    select
        orderid as order_id,
        max(created) as payment_finalized_date,
        sum(amount) / 100.0 as total_amount_paid
    from payments
    where status <> 'fail'
    group by 1

),

paid_orders as (

    select
        orders.id as order_id,
        orders.user_id as customer_id,
        orders.order_date as order_placed_at,
        orders.status as order_status,
        sum(aggregated_payments.total_amount_paid) over (partition by customers.id) as customer_lifetime_value,
        first_value(orders.order_date) over (partition by customers.id) as first_order_date,
        aggregated_payments.total_amount_paid,
        aggregated_payments.payment_finalized_date,
        customers.first_name as customer_first_name,
        customers.last_name as customer_last_name
    from orders
    left join aggregated_payments on orders.id = aggregated_payments.order_id
    left join customers on orders.user_id = customers.id

),

final as (

    select
        *,
        row_number() over (order by order_id) as transaction_seq,
        row_number() over (
            partition by customer_id order by order_id
        ) as customer_sales_seq,
        case
            when (
                rank() over (
                    partition by customer_id
                    order by order_placed_at, order_id
                ) = 1
            ) then 'new'
        else 'return' end as nvsr
    from paid_orders
    order by order_id
)

select *
from final