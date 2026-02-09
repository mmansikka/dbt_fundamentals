{{ config(materialized="view") }}

with

-- stagings
orders as (
    
    select *
    from {{ ref('stg_jaffle_shop__orders') }}

),

customers as (
    
    select *
    from {{ ref('stg_jaffle_shop__customers') }}

),

payments as (
    
    select *
    from {{ ref('stg_stripe__payments') }}

),

-- marts
aggregated_payments as (

    select
        order_id,
        max(payment_created_at) as payment_finalized_date,
        sum(payment_amount) as total_amount_paid
    from payments
    where payment_status <> 'fail'
    group by 1

),

paid_orders as (

    select
        orders.order_id,
        orders.customer_id,
        orders.order_placed_at,
        orders.order_status,
        sum(aggregated_payments.total_amount_paid) over (partition by customers.customer_id) as customer_lifetime_value,
        first_value(orders.order_placed_at) over (partition by customers.customer_id) as first_order_date,
        aggregated_payments.total_amount_paid,
        aggregated_payments.payment_finalized_date,
        customers.customer_first_name,
        customers.customer_last_name
    from orders
    left join aggregated_payments on orders.order_id = aggregated_payments.order_id
    left join customers on orders.customer_id = customers.customer_id

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