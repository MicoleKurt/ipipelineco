select
    id as customer_id,
    name as customer_name,
    lower(email) as email,
    upper(country) as country_code,
    updated_at
from {{ source('raw', 'customers') }}
