select
    id as order_id,
    customer_id,
    lower(status) as status,
    amount,
    upper(currency) as currency,
    created_at,
    updated_at
from {{ source('raw', 'orders') }}
