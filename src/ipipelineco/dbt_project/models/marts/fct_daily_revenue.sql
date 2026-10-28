select
    cast(created_at as date) as order_date,
    count(*) as orders,
    sum(amount) filter (where status <> 'cancelled') as gross_amount,
    sum(settled_amount) as settled_amount
from {{ ref('fct_orders') }}
group by 1
