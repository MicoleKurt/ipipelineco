select
    c.customer_id,
    c.customer_name,
    c.email,
    c.country_code,
    count(o.order_id) as lifetime_orders,
    coalesce(sum(o.amount) filter (where o.status <> 'cancelled'), 0) as lifetime_value
from {{ ref('stg_customers') }} c
left join {{ ref('stg_orders') }} o using (customer_id)
group by all
