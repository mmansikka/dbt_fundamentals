with src as (
    select *
    from {{ ref('stg_stripe__payments') }}
)

select sum(amount) as total_amount
from src
where status = 'success'