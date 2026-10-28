select
    payment_id,
    order_id,
    lower(method) as method,
    lower(status) as status,
    amount,
    updated_at
from {{ source('raw', 'payments') }}
