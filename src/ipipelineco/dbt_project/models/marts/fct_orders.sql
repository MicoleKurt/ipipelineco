with settled as (
    select order_id, sum(amount) as settled_amount
    from {{ ref('stg_payments') }}
    where status = 'settled'
    group by order_id
)
select
    o.order_id,
    o.customer_id,
    o.status,
    o.amount,
    o.created_at,
    coalesce(s.settled_amount, 0) as settled_amount,
    s.order_id is not null as has_settled_payment
from {{ ref('stg_orders') }} o
left join settled s using (order_id)
