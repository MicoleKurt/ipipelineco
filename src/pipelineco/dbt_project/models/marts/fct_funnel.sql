select
    event_date,
    count(distinct customer_id) filter (where event_type = 'page_view') as viewers,
    count(distinct customer_id) filter (where event_type = 'add_to_cart') as carts,
    count(distinct customer_id) filter (where event_type = 'checkout') as checkouts,
    count(distinct customer_id) filter (where event_type = 'purchase') as purchasers
from {{ ref('stg_events') }}
group by 1
