select
    event_id,
    user_id as customer_id,
    type as event_type,
    ts as event_at,
    cast(ts as date) as event_date
from {{ source('raw', 'events') }}
